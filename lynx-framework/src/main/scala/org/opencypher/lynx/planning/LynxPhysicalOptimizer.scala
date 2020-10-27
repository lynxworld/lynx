package org.opencypher.lynx.planning

import org.opencypher.lynx.LynxPlannerContext
import org.opencypher.okapi.trees.TopDown

object LynxPhysicalOptimizer {

  def process(input: PhysicalOperator)(implicit context: LynxPlannerContext): PhysicalOperator = {
    InsertCachingOperators(input)
  }

  object InsertCachingOperators {

    def apply(input: PhysicalOperator): PhysicalOperator = {
      val replacements = calculateReplacementMap(input).filterKeys {
        case _: Start => false
        case _ => true
      }

      val nodesToReplace = replacements.keySet

      TopDown[PhysicalOperator] {
        case cache: Cache => cache
        case parent if (parent.childrenAsSet intersect nodesToReplace).nonEmpty =>
          val newChildren = parent.children.map(c => replacements.getOrElse(c, c))
          parent.withNewChildren(newChildren)
      }.transform(input)
    }

    private def calculateReplacementMap(input: PhysicalOperator): Map[PhysicalOperator, PhysicalOperator] = {
      val opCounts = identifyDuplicates(input)
      val opsByHeight = opCounts.keys.toSeq.sortWith((a, b) => a.height > b.height)
      val (opsToCache, _) = opsByHeight.foldLeft(Set.empty[PhysicalOperator] -> opCounts) { (agg, currentOp) =>
        agg match {
          case (currentOpsToCache, currentCounts) =>
            val currentOpCount = currentCounts(currentOp)
            if (currentOpCount > 1) {
              val updatedOps = currentOpsToCache + currentOp
              // We're traversing `opsByHeight` from largest to smallest query sub-tree.
              // We pick the trees with the largest height for caching first, and then reduce the duplicate count
              // for the sub-trees of the cached tree by the number of times the parent tree appears.
              // The idea behind this is that if the parent was already cached, there is no need to additionally
              // cache all its children (unless they're used with a different parent somewhere else).
              val updatedCounts = currentCounts.map {
                case (op, count) => op -> (if (currentOp.containsTree(op)) count - currentOpCount else count)
              }
              updatedOps -> updatedCounts
            } else {
              currentOpsToCache -> currentCounts
            }
        }
      }
      opsToCache.map(op => op -> Cache(op)).toMap
    }

    private def identifyDuplicates(input: PhysicalOperator): Map[PhysicalOperator, Int] = {
      input
        .foldLeft(Map.empty[PhysicalOperator, Int].withDefaultValue(0)) {
          case (agg, op) => agg.updated(op, agg(op) + 1)
        }
        .filter(_._2 > 1)
    }
  }
}
