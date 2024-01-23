package org.grapheco.lynx.physical.planner.translators

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.physical.planner.PPTNodeTranslator
import org.grapheco.lynx.physical._
import org.grapheco.lynx.physical.plans.PhysicalPlan
import org.opencypher.v9_0.expressions.{EveryPath, Expression, LabelName, LogicalVariable, NodePattern, Pattern, Range, RelTypeName, RelationshipChain, RelationshipPattern, SemanticDirection}
import org.opencypher.v9_0.util.symbols.{CTNode, CTRelationship}

import scala.collection.mutable.ArrayBuffer

case class PPTCreateTranslator(p: Pattern) extends PPTNodeTranslator {
  def translate(in: Option[PhysicalPlan])(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    val definedVars = in.map(_.schema.map(_._1)).getOrElse(Seq.empty).toSet
    val (schemaLocal, ops) = p.patternParts.foldLeft((Seq.empty[(String, LynxType)], Seq.empty[FormalElement])) {
      (result, part) =>
        val (schema1, ops1) = result
        part match {
          case EveryPath(element) =>
            element match {
              //create (n)
              case NodePattern(var1: Option[LogicalVariable], labels1, properties1, _) =>
                val leftNodeName = var1.map(_.name).getOrElse(s"__NODE_${element.hashCode}")
                (schema1 :+ (leftNodeName -> CTNode)) ->
                  (ops1 :+ FormalNode(leftNodeName, labels1, properties1))

              //create (m)-[r]-(n)
              case chain: RelationshipChain =>
                val (_, schema2, ops2) = build(chain, definedVars)
                (schema1 ++ schema2) -> (ops1 ++ ops2)
            }
        }
    }

    PPTCreate(schemaLocal, ops)(in, plannerContext)
  }

  //returns (varLastNode, schema, ops)
  private def build(chain: RelationshipChain, definedVars: Set[String]): (String, Seq[(String, LynxType)], Seq[FormalElement]) = {
    val RelationshipChain(
    left,
    rp@RelationshipPattern(var2: Option[LogicalVariable], types: Seq[RelTypeName], length: Option[Option[Range]], properties2: Option[Expression], direction: SemanticDirection, legacyTypeSeparator: Boolean, baseRel: Option[LogicalVariable]),
    rnp@NodePattern(var3, labels3: Seq[LabelName], properties3: Option[Expression], _)
    ) = chain

    val varRelation = var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rp.hashCode}")
    val varRightNode = var3.map(_.name).getOrElse(s"__NODE_${rnp.hashCode}")

    val schemaLocal = ArrayBuffer[(String, LynxType)]()
    val opsLocal = ArrayBuffer[FormalElement]()
    left match {
      //create (m)-[r]-(n), left=n
      case NodePattern(var1: Option[LogicalVariable], labels1, properties1, _) =>
        val varLeftNode = var1.map(_.name).getOrElse(s"__NODE_${left.hashCode}")
        if (!definedVars.contains(varLeftNode)) {
          schemaLocal += varLeftNode -> CTNode
          opsLocal += FormalNode(varLeftNode, labels1, properties1)
        }

        if (!definedVars.contains(varRightNode)) {
          schemaLocal += varRightNode -> CTNode
          opsLocal += FormalNode(varRightNode, labels3, properties3)
        }

        schemaLocal += varRelation -> CTRelationship
        opsLocal += FormalRelationship(varRelation, types, properties2, varLeftNode, varRightNode)

        (varRightNode, schemaLocal, opsLocal)

      //create (m)-[p]-(t)-[r]-(n), leftChain=(m)-[p]-(t)
      case leftChain: RelationshipChain =>
        //build (m)-[p]-(t)
        val (varLastNode, schema, ops) = build(leftChain, definedVars)
        (
          varRightNode,
          schema ++ Seq(varRelation -> CTRelationship) ++ (if (!definedVars.contains(varRightNode)) {
            Seq(varRightNode -> CTNode)
          } else {
            Seq.empty
          }),
          ops ++ (if (!definedVars.contains(varRightNode)) {
            Seq(FormalNode(varRightNode, labels3, properties3))
          } else {
            Seq.empty
          }) ++ Seq(
            FormalRelationship(varRelation, types, properties2, varLastNode, varRightNode)
          )
        )
    }
  }
}
