package org.grapheco.lynx.parser

import org.grapheco.lynx.procedure.ProcedureExpression
import org.grapheco.lynx.runner.CypherRunnerContext
import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.ast.semantics.{SemanticErrorDef, SemanticFeature, SemanticState}
import org.opencypher.v9_0.expressions.FunctionInvocation
import org.opencypher.v9_0.frontend.PlannerName
import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.opencypher.v9_0.frontend.phases._
import org.opencypher.v9_0.rewriting.Deprecations.V2
import org.opencypher.v9_0.rewriting.rewriters.Never
import org.opencypher.v9_0.rewriting.{AstRewritingMonitor, RewriterStepSequencer}
import org.opencypher.v9_0.util._

import scala.reflect.ClassTag

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

  case class FunctionMapper(runnerContext: CypherRunnerContext) extends Phase[BaseContext, BaseState, BaseState] {
    override def phase: CompilationPhase = AST_REWRITE

    override def description: String = "map functions to their procedure implementations"

    override def process(from: BaseState, ignored: BaseContext): BaseState = {
      val rewriter = inSequence(
        bottomUp(Rewriter.lift {
          case func: FunctionInvocation => ProcedureExpression(func)(runnerContext)
        }))
      val newStatement = from.statement().endoRewrite(rewriter)
      from.withStatement(newStatement)
    }

    override def postConditions: Set[Condition] = Set.empty
  }

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

