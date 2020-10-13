package org.opencypher.lynx.planning

import cats.data.NonEmptyList
import org.opencypher.lynx._
import org.opencypher.lynx
import org.opencypher.okapi.api.graph.{Pattern, PatternElement, QualifiedGraphName, RelationshipPattern}
import org.opencypher.okapi.api.types.{CTAny, CTNode, CTNull, CTRelationship}
import org.opencypher.okapi.impl.exception.{NotImplementedException, SchemaException, UnsupportedOperationException}
import org.opencypher.okapi.impl.types.CypherTypeUtils.RichCypherType
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr.PrefixId.GraphIdPrefix
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, RelType}
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.logical.{impl => logical}
import ConstructGraphPlanner._
import org.opencypher.lynx.graph.{LynxPropertyGraph}
import org.opencypher.okapi.ir.impl.util.VarConverters.RichPatternElement

object LynxPhysicalPlanner {
  def process(input: LogicalOperator)(implicit context: LynxPlannerContext): PhysicalOperator = {

    input match {
      case logical.CartesianProduct(lhs, rhs, _) =>
        process(lhs).join(process(rhs), Seq.empty, CrossJoin)

      case logical.Select(fields, in, _) =>
        val inOp = process(in)

        val selectExpressions = fields
          .flatMap(inOp.recordHeader.ownedBy)
          .distinct

        inOp.select(selectExpressions: _*)

      case logical.Project(projectExpr, in, _) =>
        val inOp = process(in)
        val (expr, maybeAlias) = projectExpr
        val containsExpr = inOp.recordHeader.contains(expr)

        maybeAlias match {
          case Some(alias) if containsExpr => inOp.alias(expr as alias)
          case Some(alias) => inOp.add(expr as alias)
          case None => inOp.add(expr)
        }

      case logical.EmptyRecords(fields, in, _) =>
        lynx.EmptyRecords(process(in), fields)

      case logical.Start(graph, _) => lynx.Start(graph.qualifiedGraphName)

      case logical.DrivingTable(graph, _, _) => lynx.Start(graph.qualifiedGraphName, context.maybeInputRecords)

      case logical.FromGraph(graph, in, _) =>
        val inOp = process(in)
        graph match {
          case g: LogicalCatalogGraph => lynx.FromCatalogGraph(inOp, g)
          case construct: LogicalPatternGraph => planConstructGraph(inOp, construct)
        }

      case logical.Unwind(list, item, in, _) =>
        val explodeExpr = Explode(list)
        process(in).add(explodeExpr as item)

      case logical.PatternScan(pattern, mapping, in, _) =>
        planScan(
          Some(process(in)),
          in.graph,
          pattern,
          mapping
        )

      case logical.Aggregate(aggregations, group, in, _) => lynx.Aggregate(process(in), group, aggregations)

      case logical.Filter(expr, in, _) => process(in).filter(expr)

      case logical.ValueJoin(lhs, rhs, predicates, _) =>
        val joinExpressions = predicates.map(p => p.lhs -> p.rhs).toSeq
        process(lhs).join(process(rhs), joinExpressions, InnerJoin)

      case logical.Distinct(fields, in, _) =>
        val elementExprs: Set[Var] = Set(fields.toSeq: _*)
        lynx.Distinct(process(in), elementExprs)

      case logical.TabularUnionAll(left, right) =>
        process(left).unionAll(process(right))

      case logical.GraphUnionAll(left, right) =>
        process(left).graphUnionAll(process(right))

      case logical.Expand(source, rel, target, direction, sourceOp, targetOp, _) =>
        val first = process(sourceOp)
        val third = process(targetOp)

        val relPattern = RelationshipPattern(rel.cypherType.toCTRelationship)
        val second = planScan(
          None,
          sourceOp.graph,
          relPattern,
          Map(rel -> relPattern.relElement)
        )

        val startNode = StartNode(rel)(CTNode)
        val endNode = EndNode(rel)(CTNode)

        direction match {
          case org.opencypher.okapi.logical.impl.Outgoing =>
            val tempResult = first.join(second, Seq(source -> startNode), InnerJoin)
            tempResult.join(third, Seq(endNode -> target), InnerJoin)

          case org.opencypher.okapi.logical.impl.Incoming =>
            val tempResult = third.join(second, Seq(target -> endNode), InnerJoin)
            tempResult.join(first, Seq(startNode -> source), InnerJoin)

          case Undirected =>
            val tempOutgoing = first.join(second, Seq(source -> startNode), InnerJoin)
            val outgoing = tempOutgoing.join(third, Seq(endNode -> target), InnerJoin)

            val filterExpression = Not(Equals(startNode, endNode))
            val relsWithoutLoops = second.filter(filterExpression)

            val tempIncoming = first.join(relsWithoutLoops, Seq(source -> endNode), InnerJoin)
            val incoming = tempIncoming.join(third, Seq(startNode -> target), InnerJoin)

            lynx.TabularUnionAll(outgoing, incoming)
        }

      case logical.ExpandInto(source, rel, target, direction, sourceOp, _) =>
        val in = process(sourceOp)

        val relPattern = RelationshipPattern(rel.cypherType.toCTRelationship)
        val relationships = planScan(
          None,
          sourceOp.graph,
          relPattern,
          Map(rel -> relPattern.relElement)
        )

        val startNode = StartNode(rel)(CTAny)
        val endNode = EndNode(rel)(CTAny)

        direction match {
          case Outgoing | Incoming =>
            in.join(relationships, Seq(source -> startNode, target -> endNode), InnerJoin)

          case Undirected =>
            val outgoing = in.join(relationships, Seq(source -> startNode, target -> endNode), InnerJoin)
            val incoming = in.join(relationships, Seq(target -> startNode, source -> endNode), InnerJoin)
            lynx.TabularUnionAll(outgoing, incoming)
        }

      case logical.BoundedVarLengthExpand(source, list, target, edgeScanType, direction, lower, upper, sourceOp, targetOp, _) =>

        val edgeScan = Var(list.name)(edgeScanType)

        val edgePattern = RelationshipPattern(edgeScanType)
        val edgeScanOp = planScan(
          None,
          sourceOp.graph,
          edgePattern,
          Map(edgeScan -> edgePattern.relElement)
        )

        val isExpandInto = sourceOp == targetOp

        val planner = direction match {
          case Outgoing | Incoming => new DirectedVarLengthExpandPlanner(
            source, list, edgeScan, target,
            lower, upper,
            sourceOp, edgeScanOp, targetOp,
            isExpandInto)

          case Undirected => new UndirectedVarLengthExpandPlanner(
            source, list, edgeScan, target,
            lower, upper,
            sourceOp, edgeScanOp, targetOp,
            isExpandInto)
        }

        planner.plan

      case logical.Optional(lhs, rhs, _) => planOptional(lhs, rhs)

      case logical.ExistsSubQuery(predicateField, lhs, rhs, _) =>

        val leftResult = process(lhs)
        val rightResult = process(rhs)

        val leftHeader = leftResult.recordHeader
        val rightHeader = rightResult.recordHeader

        // 0. Find common expressions, i.e. join expressions
        val joinExprs = leftHeader.vars.intersect(rightHeader.vars)
        // 1. Alias join expressions on rhs
        val renameExprs = joinExprs.map(e => e as Var(s"${e.name}${System.nanoTime}")(e.cypherType))
        val rightWithAliases = rightResult.alias(renameExprs)
        // 2. Drop Join expressions and their children in rhs
        val exprsToRemove = joinExprs.flatMap(v => rightHeader.ownedBy(v))
        val reducedRhsData = rightWithAliases.dropExprSet(exprsToRemove)
        // 3. Compute distinct rows in rhs
        val distinctRhsData = lynx.Distinct(reducedRhsData, renameExprs.map(_.alias))
        // 4. Join lhs and prepared rhs using a left outer join
        val joinedData = leftResult.join(distinctRhsData, renameExprs.map(a => a.expr -> a.alias).toSeq, LeftOuterJoin)
        // 5. If at least one rhs join column is not null, the sub-query exists and true is projected to the target expression
        val targetExpr = renameExprs.head.alias
        joinedData.addInto(IsNotNull(targetExpr) -> predicateField.targetField)

      case logical.OrderBy(sortItems: Seq[SortItem], in, _) =>
        lynx.OrderBy(process(in), sortItems)

      case logical.Skip(expr, in, _) =>
        lynx.Skip(process(in), expr)

      case logical.Limit(expr, in, _) =>
        lynx.Limit(process(in), expr)

      case logical.ReturnGraph(in, _) => lynx.ReturnGraph(process(in))

      case other => throw NotImplementedException(s"Physical planning of operator $other")
    }
  }

  def planScan(
                maybeInOp: Option[PhysicalOperator],
                logicalGraph: LogicalGraph,
                scanPattern: Pattern,
                varPatternElementMapping: Map[Var, PatternElement]
              )(implicit context: LynxPlannerContext): PhysicalOperator = {
    val inOp = maybeInOp match {
      case Some(relationalOp) => relationalOp
      case _ => lynx.Start(logicalGraph.qualifiedGraphName)
    }

    val graph: LynxPropertyGraph = logicalGraph match {
      case _: LogicalCatalogGraph =>
        inOp.context.resolveGraph(logicalGraph.qualifiedGraphName)

      case p: LogicalPatternGraph =>
        inOp.context.queryLocalCatalog.getOrElse(p.qualifiedGraphName, planConstructGraph(inOp, p).graph)
    }

    val scanOp = graph.scanOperator(scanPattern)

    val validScan = scanPattern.elements.forall { patternElement =>
      scanOp.recordHeader.elementVars.exists { headerVar =>
        headerVar.name == patternElement.name && headerVar.cypherType.withoutGraph == patternElement.cypherType.withoutGraph
      }
    }

    if (!validScan) throw SchemaException(s"Expected the scan to include Variables for all elements of ${scanPattern.elements}" +
      s" but got ${scanOp.recordHeader.elementVars}")

    scanOp
      .assignScanName(varPatternElementMapping.mapValues(_.toVar).map(_.swap))
      .switchContext(inOp.context)
  }

  // TODO: process operator outside of def
  private def planOptional(lhs: LogicalOperator, rhs: LogicalOperator)(implicit context: LynxPlannerContext): PhysicalOperator = {
    val lhsOp = process(lhs)
    val rhsOp = process(rhs)

    val lhsHeader = lhsOp.recordHeader
    val rhsHeader = rhsOp.recordHeader

    def generateUniqueName = s"tmp${System.nanoTime}"

    // 1. Compute expressions between left and right side
    val commonExpressions = lhsHeader.expressions.intersect(rhsHeader.expressions)
    val joinExprs = commonExpressions.collect { case v: Var => v }
    val otherExpressions = commonExpressions -- joinExprs

    // 2. Remove siblings of the join expressions and other common fields
    val expressionsToRemove = joinExprs
      .flatMap(v => rhsHeader.ownedBy(v) - v)
      .union(otherExpressions)
    val rhsWithDropped = lynx.Drop(rhsOp, expressionsToRemove)

    // 3. Rename the join expressions on the right hand side, in order to make them distinguishable after the join
    val joinExprRenames = joinExprs.map(e => e as Var(generateUniqueName)(e.cypherType))
    val rhsWithAlias = lynx.Alias(rhsWithDropped, joinExprRenames.toSeq)
    val rhsJoinReady = lynx.Drop(rhsWithAlias, joinExprs.collect { case e: Expr => e })

    // 4. Left outer join the left side and the processed right side
    val joined = lhsOp.join(rhsJoinReady, joinExprRenames.map(a => a.expr -> a.alias).toSeq, LeftOuterJoin)

    // 5. Select the resulting header expressions
    lynx.Select(joined, joined.recordHeader.expressions.toList)
  }

  implicit class PhysicalOperatorOps(op: PhysicalOperator) {
    private implicit def context: LynxPlannerContext = op.context

    def select(expressions: Expr*): PhysicalOperator = lynx.Select(op, expressions.toList)

    def filter(expression: Expr): PhysicalOperator = {
      if (expression == TrueLit) {
        op
      } else if (expression.cypherType == CTNull) {
        lynx.Start.fromEmptyGraph(LynxRecords.empty(op.recordHeader))
      } else {
        lynx.Filter(op, expression)
      }
    }

    /**
     * Renames physical columns to given header expression names.
     * Ensures that there is a physical column for each return item, i.e. aliases lead to duplicate physical columns.
     */
    def alignColumnsWithReturnItems: PhysicalOperator = {
      val selectExprs = op.maybeReturnItems.getOrElse(List.empty)
        .flatMap(op.recordHeader.expressionsFor)
        .toList

      val renames = selectExprs
        .map(expr => expr -> expr.withoutType.toString.replace('.', '_'))
        .toMap

      lynx.Select(op, selectExprs, renames)
    }

    def renameColumns(columnRenames: Map[Expr, String]): PhysicalOperator = {
      if (columnRenames.isEmpty) op else lynx.Select(op, op.recordHeader.expressions.toList, columnRenames)
    }

    def join(other: PhysicalOperator, joinExprs: Seq[(Expr, Expr)], joinType: JoinType): PhysicalOperator = {
      lynx.Join(op, other.withDisjointColumnNames(op.recordHeader), joinExprs, joinType)
    }

    def graphUnionAll(other: PhysicalOperator): PhysicalOperator = {
      lynx.GraphUnionAll(NonEmptyList(op, List(other)), QualifiedGraphName("UnionAllGraph"))
    }

    def unionAll(other: PhysicalOperator): PhysicalOperator = {
      val combinedHeader = op.recordHeader union other.recordHeader

      // rename all columns to make sure we have no conflicts
      val targetHeader = RecordHeader.empty.withExprs(combinedHeader.expressions)

      val elementVars = targetHeader.nodeVars ++ targetHeader.relationshipVars

      val opWithAlignedElements = elementVars.foldLeft(op) {
        case (acc, elementVar) => acc.alignExpressions(elementVar, elementVar, targetHeader)
      }.alignColumnNames(targetHeader)

      val otherWithAlignedElements = elementVars.foldLeft(other) {
        case (acc, elementVar) => acc.alignExpressions(elementVar, elementVar, targetHeader)
      }.alignColumnNames(targetHeader)

      lynx.TabularUnionAll(opWithAlignedElements, otherWithAlignedElements)
    }

    def add(values: Expr*): PhysicalOperator = {
      if (values.isEmpty) op else lynx.Add(op, values.toList)
    }

    def addInto(valueIntos: (Expr, Expr)*): PhysicalOperator = {
      if (valueIntos.isEmpty) op else lynx.AddInto(op, valueIntos.toList)
    }

    def dropExprSet[E <: Expr](expressions: Set[E]): PhysicalOperator = {
      val necessaryDrops = expressions.filter(op.recordHeader.expressions.contains)
      if (necessaryDrops.nonEmpty) {
        lynx.Drop(op, necessaryDrops)
      } else op
    }

    def dropExpressions[E <: Expr](expressions: E*): PhysicalOperator = {
      dropExprSet(expressions.toSet)
    }

    def alias(aliases: AliasExpr*): PhysicalOperator = Alias(op, aliases)

    def alias(aliases: Set[AliasExpr]): PhysicalOperator = alias(aliases.toSeq: _*)

    // Only works with single element tables
    def assignScanName(mapping: Map[Var, Var]): PhysicalOperator = {
      val aliases = mapping.map {
        case (from, to) => AliasExpr(from, to)
      }

      op.select(aliases.toList: _*)
    }

    def switchContext(context: LynxPlannerContext): PhysicalOperator = {
      SwitchContext(op, context)
    }

    def prefixVariableId(v: Var, prefix: GraphIdPrefix): PhysicalOperator = {
      val prefixedIds = op.recordHeader.idExpressions(v).map(exprToPrefix => PrefixId(ToId(exprToPrefix), prefix) -> exprToPrefix)
      op.addInto(prefixedIds.toSeq: _*)
    }

    def alignWith(inputElement: Var, targetElement: Var, targetHeader: RecordHeader): PhysicalOperator = {
      op.alignExpressions(inputElement, targetElement, targetHeader).alignColumnNames(targetHeader)
    }

    // TODO: element needs to contain all labels/relTypes: all case needs to be explicitly expanded with the schema
    /**
     * Aligns a single element within the operator with the given target element in the target header.
     *
     * @param inputVar     the variable of the element that should be aligned
     * @param targetVar    the variable of the reference element
     * @param targetHeader the header describing the desired state
     * @return operator with aligned element
     */
    def alignExpressions(inputVar: Var, targetVar: Var, targetHeader: RecordHeader): PhysicalOperator = {

      val targetHeaderLabels = targetHeader.labelsFor(targetVar).map(_.label.name)
      val targetHeaderTypes = targetHeader.typesFor(targetVar).map(_.relType.name)

      // Labels/RelTypes that do not need to be added
      val existingLabels = op.recordHeader.labelsFor(inputVar).map(_.label.name)
      val existingRelTypes = op.recordHeader.typesFor(inputVar).map(_.relType.name)

      val otherElements = op.recordHeader -- Set(inputVar)
      val toRetain = otherElements.expressions + (inputVar as targetVar)

      // Rename variable and select columns owned by elementVar
      val renamedElement = op.select(toRetain.toSeq: _*)

      // Drop expressions that are not in the target header
      val dropExpressions = renamedElement.recordHeader.expressions -- targetHeader.expressions
      val withDroppedExpressions = renamedElement.dropExprSet(dropExpressions)

      // Fill in missing true label columns
      val trueLabels = inputVar.cypherType match {
        case CTNode(labels, _) => (targetHeaderLabels intersect labels) -- existingLabels
        case _ => Set.empty
      }
      val withTrueLabels = withDroppedExpressions.addInto(
        trueLabels.map(label => TrueLit -> HasLabel(targetVar, Label(label))).toSeq: _*
      )

      // Fill in missing false label columns
      val falseLabels = targetVar.cypherType match {
        case n if n.subTypeOf(CTNode.nullable) => targetHeaderLabels -- trueLabels -- existingLabels
        case _ => Set.empty
      }
      val withFalseLabels = withTrueLabels.addInto(
        falseLabels.map(label => FalseLit -> HasLabel(targetVar, Label(label))).toSeq: _*
      )

      // Fill in missing true relType columns
      val trueRelTypes = inputVar.cypherType match {
        case CTRelationship(relTypes, _) => (targetHeaderTypes intersect relTypes) -- existingRelTypes
        case _ => Set.empty
      }
      val withTrueRelTypes = withFalseLabels.addInto(
        trueRelTypes.map(relType => TrueLit -> HasType(targetVar, RelType(relType))).toSeq: _*
      )

      // Fill in missing false relType columns
      val falseRelTypes = targetVar.cypherType match {
        case r if r.subTypeOf(CTRelationship.nullable) => targetHeaderTypes -- trueRelTypes -- existingRelTypes
        case _ => Set.empty
      }
      val withFalseRelTypes = withTrueRelTypes.addInto(
        falseRelTypes.map(relType => FalseLit -> HasType(targetVar, RelType(relType))).toSeq: _*
      )

      // Fill in missing properties
      val missingProperties = targetHeader.propertiesFor(targetVar) -- withFalseRelTypes.recordHeader.propertiesFor(targetVar)
      val withProperties = withFalseRelTypes.addInto(
        missingProperties.map(propertyExpr => NullLit -> propertyExpr).toSeq: _*
      )

      import Expr._
      assert(targetHeader.expressionsFor(targetVar) == withProperties.recordHeader.expressionsFor(targetVar),
        s"""Expected header expressions for $targetVar:
           |\t${targetHeader.expressionsFor(targetVar).toSeq.sorted.mkString(", ")},
           |got
           |\t${withProperties.recordHeader.expressionsFor(targetVar).toSeq.sorted.mkString(", ")}""".stripMargin)
      withProperties
    }

    /**
     * Returns an operator with renamed columns such that the operators columns do not overlap with the other header's
     * columns.
     *
     * @param otherHeader header from which the column names should be disjoint
     * @return operator with disjoint column names
     */
    def withDisjointColumnNames(otherHeader: RecordHeader): PhysicalOperator = {
      val header = op.recordHeader
      val conflictingExpressions = header.expressions.filter(e => otherHeader.columns.contains(header.column(e)))

      if (conflictingExpressions.isEmpty) {
        op
      } else {
        val renameMapping = conflictingExpressions.foldLeft(Map.empty[Expr, String]) {
          case (acc, nextRename) =>
            val newColumnName = header.newConflictFreeColumnName(nextRename, otherHeader.columns ++ acc.values)
            acc + (nextRename -> newColumnName)
        }
        op.renameColumns(renameMapping)
      }
    }

    /**
     * Ensures that the column names are aligned with the target header.
     *
     * @note All expressions in the operator header must be present in the target header.
     * @param targetHeader the header with which the column names should be aligned with
     * @return operator with aligned column names
     */
    def alignColumnNames(targetHeader: RecordHeader): PhysicalOperator = {
      val exprsNotInTarget = op.recordHeader.expressions -- targetHeader.expressions
      require(exprsNotInTarget.isEmpty,
        s"""|Column alignment requires for all header expressions to be present in the target header:
            |Current: ${op.recordHeader}
            |Target: $targetHeader
            |Missing expressions: ${exprsNotInTarget.mkString(", ")}
        """.stripMargin)

      if (op.recordHeader.expressions.forall(expr => op.recordHeader.column(expr) == targetHeader.column(expr))) {
        op
      } else {
        val columnRenames = op.recordHeader.expressions.foldLeft(Map.empty[Expr, String]) {
          case (currentMap, expr) => currentMap + (expr -> targetHeader.column(expr))
        }
        op.renameColumns(columnRenames)
      }
    }

    def singleElement: Var = {
      op.recordHeader.elementVars.toList match {
        case element :: Nil => element
        case Nil => throw SchemaException(s"Operation requires single element table, input contains no elements")
        case other => throw SchemaException(s"Operation requires single element table, found ${other.mkString("[", ", ", "]")}")
      }
    }
  }

}