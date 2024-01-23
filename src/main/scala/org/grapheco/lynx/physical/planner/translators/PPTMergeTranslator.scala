package org.grapheco.lynx.physical.planner.translators

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.physical.planner.PPTNodeTranslator
import org.grapheco.lynx.physical.plans.{Merge, PhysicalPlan}
import org.grapheco.lynx.physical._
import org.opencypher.v9_0.ast.{MergeAction, OnCreate, OnMatch}
import org.opencypher.v9_0.expressions.{EveryPath, Expression, LabelName, LogicalVariable, NodePattern, Pattern, Range, RelTypeName, RelationshipChain, RelationshipPattern, SemanticDirection}
import org.opencypher.v9_0.util.symbols.{CTNode, CTRelationship}

import scala.collection.mutable

case class PPTMergeTranslator(p: Pattern, a: Seq[MergeAction]) extends PPTNodeTranslator {
  def translate(in: Option[PhysicalPlan])(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    val definedVars = in.map(_.schema.map(_._1)).getOrElse(Seq.empty).toSet
    val mergeOps = mutable.ArrayBuffer[FormalElement]()
    val mergeSchema = mutable.ArrayBuffer[(String, LynxType)]()

    p.patternParts.foreach {
      case EveryPath(element) => {
        element match {
          case NodePattern(var1: Option[LogicalVariable], labels1, properties1, _) => {
            val leftNodeName = var1.map(_.name).getOrElse(s"__NODE_${element.hashCode}")
            mergeSchema.append((leftNodeName, CTNode))
            mergeOps.append(FormalNode(leftNodeName, labels1, properties1))
          }
          case chain: RelationshipChain => {
            buildMerge(chain, definedVars, mergeSchema, mergeOps)
          }
        }
      }
    }

    Merge(mergeSchema,
      mergeOps,
      a collect { case m: OnMatch => m },
      a collect { case c: OnCreate => c })(in, plannerContext)
  }

  private def buildMerge(chain: RelationshipChain, definedVars: Set[String], mergeSchema: mutable.ArrayBuffer[(String, LynxType)], mergeOps: mutable.ArrayBuffer[FormalElement]): String = {
    val RelationshipChain(
    left,
    rp@RelationshipPattern(var2: Option[LogicalVariable], types: Seq[RelTypeName], length: Option[Option[Range]], properties2: Option[Expression], direction: SemanticDirection, legacyTypeSeparator: Boolean, baseRel: Option[LogicalVariable]),
    rnp@NodePattern(var3, labels3: Seq[LabelName], properties3: Option[Expression], _)
    ) = chain

    val varRelation = var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rp.hashCode}")
    val varRightNode = var3.map(_.name).getOrElse(s"__NODE_${rnp.hashCode}")
    left match {
      //create (m)-[r]-(n), left=n
      case NodePattern(var1: Option[LogicalVariable], labels1, properties1, _) =>
        val varLeftNode = var1.map(_.name).getOrElse(s"__NODE_${left.hashCode}")
        mergeSchema.append((varLeftNode, CTNode))
        mergeSchema.append((varRelation, CTRelationship))
        mergeSchema.append((varRightNode, CTNode))

        mergeOps.append(FormalNode(varLeftNode, labels1, properties1))
        mergeOps.append(FormalRelationship(varRelation, types, properties2, varLeftNode, varRightNode)) // direction
        mergeOps.append(FormalNode(varRightNode, labels3, properties3))

        varRightNode

      // (m)-[p]-(t)-[r]-(n), leftChain=(m)-[p]-(t)
      case leftChain: RelationshipChain =>
        // (m)-[p]-(t)
        val lastNode = buildMerge(leftChain, definedVars, mergeSchema, mergeOps)
        mergeSchema.append((varRelation, CTRelationship))
        mergeSchema.append((varRightNode, CTNode))

        mergeOps.append(FormalRelationship(varRelation, types, properties2, lastNode, varRightNode)) // direction
        mergeOps.append(FormalNode(varRightNode, labels3, properties3))

        varRightNode
    }

  }
}
