package org.grapheco.lynx

import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.ast.semantics.{SemanticErrorDef, SemanticFeature, SemanticState}
import org.opencypher.v9_0.frontend.PlannerName
import org.opencypher.v9_0.frontend.phases.{AstRewriting, BaseContains, BaseContext, BaseState, CompilationPhaseTracer, InitialState, InternalNotificationLogger, Monitors, Parsing, SemanticAnalysis, SyntaxDeprecationWarnings, Transformer, devNullLogger, _}
import org.opencypher.v9_0.rewriting.Deprecations.V2
import org.opencypher.v9_0.rewriting.rewriters.Never
import org.opencypher.v9_0.rewriting.{AstRewritingMonitor, RewriterStepSequencer}
import org.opencypher.v9_0.util.spi.MapToPublicExceptions
import org.opencypher.v9_0.util.{CypherException, InputPosition}

import scala.collection.mutable
import scala.reflect.ClassTag

trait QueryParser {
  def parse(query: String): (Statement, Map[String, Any], SemanticState)
}

class CachedQueryParser(parser: QueryParser) extends QueryParser {
  val cache = mutable.Map[String, (Statement, Map[String, Any], SemanticState)]()

  override def parse(query: String): (Statement, Map[String, Any], SemanticState) =
    cache.getOrElseUpdate(query, parser.parse(query))
}

class DefaultQueryParser(runnerContext: CypherRunnerContext) extends QueryParser {
  val context = new BaseContext() {
    override def tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING

    override def notificationLogger: InternalNotificationLogger = devNullLogger

    override def monitors: Monitors = new Monitors {
      override def newMonitor[T <: AnyRef : ClassTag](tags: String*): T = {
        new AstRewritingMonitor {
          override def abortedRewriting(obj: AnyRef): Unit = ()

          override def abortedRewritingDueToLargeDNF(obj: AnyRef): Unit = ()
        }
      }.asInstanceOf[T]

      override def addMonitorListener[T](monitor: T, tags: String*): Unit = ()
    }

    override def errorHandler: Seq[SemanticErrorDef] => Unit = errors => {}

    override def exceptionCreator: (String, InputPosition) => CypherException = new LynxCypherException(_, _)
  }

  protected val transformers: Transformer[BaseContext, BaseState, BaseState] =
    Parsing.adds(BaseContains[Statement]) andThen
      SyntaxDeprecationWarnings(V2) andThen
      PreparatoryRewriting(V2) andThen
      SemanticAnalysis(warn = true, SemanticFeature.Cypher10Support, SemanticFeature.MultipleGraphs, SemanticFeature.WithInitialQuerySignature)
        .adds(BaseContains[SemanticState]) andThen
      AstRewriting(RewriterStepSequencer.newPlain, Never, getDegreeRewriting = false) andThen
      isolateAggregation andThen
      SemanticAnalysis(warn = false, SemanticFeature.Cypher10Support, SemanticFeature.MultipleGraphs, SemanticFeature.WithInitialQuerySignature) andThen
      Namespacer andThen
      CNFNormalizer andThen
      LateAstRewriting andThen
      FunctionMapper(runnerContext)

  override def parse(query: String): (Statement, Map[String, Any], SemanticState) = {
    val startState = InitialState(query, None, new PlannerName {
      override def name: String = "lynx"

      override def toTextOutput: String = s"$name $version"

      override def version: String = org.grapheco.lynx.version

    })

    val endState = transformers.transform(startState, context)
    val params = endState.extractedParams
    val rewritten = endState.statement
    (rewritten, params, endState.maybeSemantics.get)
  }
}

class LynxCypherException(msg: String, position: InputPosition) extends CypherException(msg, null) with LynxException {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]): T = ???
}