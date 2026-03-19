package org.grapheco.lynx.infer

import org.grapheco.lynx.infer.cache.core.{CachePolicy, InferCache, Meta, NoneInferCache}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship}

sealed trait InferExecutor

trait CacheInferExecutor {
  def cache: InferCache
  def cacheAvailable: Boolean = cache != NoneInferCache
}

abstract class InferExpandExecutor extends InferExecutor {
  def infer(node: LynxNode): Seq[(LynxRelationship, LynxNode)]
}

abstract class InferPropertyExecutor extends InferExecutor {
  def infer(node: LynxNode, props: Seq[LynxPropertyKey] = Seq.empty): LynxNode
}

abstract class InferLabelExecutor extends InferExecutor {
  def infer(node: LynxNode): LynxNode
}

abstract class InferLinkExecutor extends InferExecutor {
  def infer(node: LynxNode, nodes: Seq[LynxNode]): Seq[(LynxNode, LynxRelationship, LynxNode)]
}

abstract class CacheInferExpandExecutor extends InferExpandExecutor with CacheInferExecutor {
  val relType: Int

  val inferCode: Int = this.hashCode() + relType

  override def infer(node: LynxNode): Seq[(LynxRelationship, LynxNode)] = if (cacheAvailable) {

    cache.getExpand(node.id.toLynxInteger.v, inferCode) match {
      case cachedValues if cachedValues.nonEmpty =>
        cachedValues
      case _ => {
        val (result, cost) = costInfer(node)
        cache.putExpand(node.id.toLynxInteger.v, inferCode, result.toList, cost)
        result
      }
    }
  } else {
    _infer(node)
  }

  private def costInfer(node: LynxNode): (Seq[(LynxRelationship, LynxNode)], Long) = {
    val t0 = System.currentTimeMillis()
    val r = _infer(node)
    (r, System.currentTimeMillis() - t0)
  }

  def _infer(node: LynxNode): Seq[(LynxRelationship, LynxNode)]

}

abstract class CacheInferLinkExecutor extends InferLinkExecutor with CacheInferExecutor {
  val relType: Int

  val inferCode: Int = this.hashCode() + relType

  val getNodeById: LynxInteger => Option[LynxNode]

  override def infer(node: LynxNode, nodes: Seq[LynxNode]): Seq[(LynxNode, LynxRelationship, LynxNode)] = if (cacheAvailable) {

    cache.getLink(node.id.toLynxInteger.v, inferCode) match {
      case cachedValues if cachedValues.nonEmpty =>
        cachedValues.map(c => (node, c._1, getNodeById(c._2).get))
      case _ => {
        val (result, cost) = costInfer(node)
        cache.putLink(node.id.toLynxInteger.v, inferCode, result.map(r => r._2 -> r._3.id.toLynxInteger).toList, cost)
        result
      }
    }
  } else {
    _infer(node)
  }

  private def costInfer(node: LynxNode): (Seq[(LynxNode, LynxRelationship, LynxNode)], Long) = {
    val t0 = System.currentTimeMillis()
    val r = _infer(node)
    (r, System.currentTimeMillis() - t0)
  }

  def _infer(node: LynxNode): Seq[(LynxNode, LynxRelationship, LynxNode)]

}

abstract class CacheInferPropertyExecutor extends InferPropertyExecutor with CacheInferExecutor {

  def inferCode(props: Seq[LynxPropertyKey]): Int = this.hashCode()+props.map(_.value.hashCode).sum

  override def infer(node: LynxNode, props: Seq[LynxPropertyKey]): LynxNode = if(cacheAvailable) {
    cache.getProp(node.id.toLynxInteger.v, inferCode(props)) match {
      case cachedProps if cachedProps.nonEmpty =>
        updateNode(node, cachedProps)
      case _ => {
        val (result, cost) = costInfer(node, props)
        val toCache = props.map(p => (p, result.property(p).get)).toList
        cache.putProp(node.id.toLynxInteger.v, inferCode(props), toCache, cost)
        return result
      }
    }
    node
  } else {
    _infer(node, props)
  }

  def updateNode(node: LynxNode, props: List[(LynxPropertyKey, LynxValue)]): LynxNode

  private def costInfer(node: LynxNode, props: Seq[LynxPropertyKey]): (LynxNode, Long) = {
    val t0 = System.currentTimeMillis()
    val r = _infer(node, props)
    (r, System.currentTimeMillis() - t0)
  }

  def _infer(node: LynxNode, props: Seq[LynxPropertyKey]): LynxNode

}

abstract class CacheInferLabelExecutor extends InferLabelExecutor with CacheInferExecutor {

  val inferCode: Int = this.hashCode() + Meta.LABEL

  override def infer(node: LynxNode): LynxNode = if(cacheAvailable) {
    cache.getProp(node.id.toLynxInteger.v, inferCode) match {
      case cachedProps if cachedProps.nonEmpty =>
        cachedProps.map(_._2).collect{case v: LynxString => v.v}.map(LynxNodeLabel(_)).foldLeft(node)((n, l) => updateNode(n, l))
      case _ => {
        val (result, cost) = costInfer(node)
        val toCache = result.labels.map(l => LynxPropertyKey("$LABEL") -> LynxString(l.value)).toList
        cache.putProp(node.id.toLynxInteger.v, inferCode, toCache, cost)
        return result
      }
    }
    node
  } else {
    _infer(node)
  }

  def updateNode(node: LynxNode, label: LynxNodeLabel): LynxNode

  private def costInfer(node: LynxNode): (LynxNode, Long) = {
    val t0 = System.currentTimeMillis()
    val r = _infer(node)
    (r, System.currentTimeMillis() - t0)
  }

  def _infer(node: LynxNode): LynxNode

}


//abstract class CacheInferLinkExecutor extends InferLinkExecutor with CacheInferExecutor {
//  def cacheKey(node: LynxNode): CacheKey.Expand
//
//  override def infer(node: LynxNode, nodes: Seq[LynxNode]): Seq[(LynxNode, LynxRelationship, LynxNode)] = if (cacheAvailable) {
//    val key = cacheKey(node)
//    cache.get(key) match {
//      case Some(v: CacheValue.ExpandValue) => v.values
//      case None => {
//        val (result, cost) = costInfer(node)
//        cache.put(key, CacheValue.ExpandValue(result.toList), cost)
//        result
//      }
//      case e => throw LynxException("Unknown cache error, value is: "+e.toString)
//    }
//  } else {
//    _infer(node)
//  }
//
//  private def costInfer(node: LynxNode, nodes: Seq[LynxNode]): (Seq[(LynxNode, LynxRelationship, LynxNode)], Long) = {
//    val t0 = System.currentTimeMillis()
//    val r = _infer(node)
//    (r, System.currentTimeMillis() - t0)
//  }
//
//  def _infer(node: LynxNode, nodes: Seq[LynxNode]): Seq[(LynxNode, LynxRelationship, LynxNode)]
//
//}
