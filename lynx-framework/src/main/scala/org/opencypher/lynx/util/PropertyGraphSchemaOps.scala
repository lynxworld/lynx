package org.opencypher.lynx.util

import org.opencypher.lynx.RecordHeader
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.types.CypherTypeUtils.RichCypherType
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey, RelType}

object PropertyGraphSchemaOps {

  implicit class PropertyGraphSchemaOps(val schema: PropertyGraphSchema) extends AnyVal {

    def headerForElement(element: Var, exactLabelMatch: Boolean = false): RecordHeader = {
      element.cypherType match {
        case _: CTNode => schema.headerForNode(element, exactLabelMatch)
        case _: CTRelationship => schema.headerForRelationship(element)
        case other => throw IllegalArgumentException("Element", other)
      }
    }

    def headerForNode(node: Var, exactLabelMatch: Boolean = false): RecordHeader = {
      val labels: Set[String] = node.cypherType.toCTNode.labels
      headerForNode(node, labels, exactLabelMatch)
    }

    def headerForNode(node: Var, labels: Set[String], exactLabelMatch: Boolean): RecordHeader = {
      val labelCombos = if (exactLabelMatch) {
        Set(labels)
      } else {
        if (labels.isEmpty) {
          // all nodes scan
          schema.allCombinations
        } else {
          // label scan
          val impliedLabels = schema.impliedLabels.transitiveImplicationsFor(labels)
          schema.combinationsFor(impliedLabels)
        }
      }

      val labelExpressions: Set[Expr] = labelCombos.flatten.map { label =>
        HasLabel(node, Label(label))
      }

      val propertyExpressions = schema.nodePropertyKeysForCombinations(labelCombos).map {
        case (k, t) => ElementProperty(node, PropertyKey(k))(t)
      }

      RecordHeader.from(labelExpressions ++ propertyExpressions + node)
    }

    def headerForRelationship(rel: Var): RecordHeader = {
      val types = rel.cypherType match {
        case CTRelationship(relTypes, _) if relTypes.isEmpty =>
          schema.relationshipTypes
        case CTRelationship(relTypes, _) =>
          relTypes
        case other =>
          throw IllegalArgumentException(CTRelationship, other)
      }

      headerForRelationship(rel, types)
    }

    def headerForRelationship(rel: Var, relTypes: Set[String]): RecordHeader = {
      val relKeyHeaderProperties = relTypes
        .flatMap(t => schema.relationshipPropertyKeys(t))
        .groupBy { case (propertyKey, _) => propertyKey }
        .mapValues { keysWithType =>
          keysWithType.toSeq.unzip match {
            case (keys, types) if keys.size == relTypes.size && types.forall(_ == types.head) => types.head
            case (_, types) => types.head.nullable
          }
        }

      val propertyExpressions: Set[Expr] = relKeyHeaderProperties.map {
        case (k, t) => ElementProperty(rel, PropertyKey(k))(t)
      }.toSet

      val startNodeExpr = StartNode(rel)(CTNode)
      val hasTypeExprs = relTypes.map(relType => HasType(rel, RelType(relType)))
      val endNodeExpr = EndNode(rel)(CTNode)

      val relationshipExpressions = hasTypeExprs ++ propertyExpressions + rel + startNodeExpr + endNodeExpr

      RecordHeader.from(relationshipExpressions)
    }
  }

}
