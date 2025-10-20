package org.grapheco.lynx.infer

import org.grapheco.lynx.LynxException
import org.grapheco.lynx.infer.cache.{CacheKey, CacheValue, InferCache, Meta, NoneInferCache}
import org.grapheco.lynx.infer.cache.core.CachePolicy
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxString
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
  val relType: Long

  def cacheKey(node: LynxNode): CacheKey.Expand = CacheKey.Expand(node.id.toLynxInteger.v, relType)

  override def infer(node: LynxNode): Seq[(LynxRelationship, LynxNode)] = if (cacheAvailable) {
    val key = cacheKey(node)
    cache.get(key) match {
      case Some(v: CacheValue.ExpandValue) => v.values
      case None => {
        val (result, cost) = costInfer(node)
        cache.put(key, CacheValue.ExpandValue(result.toList), cost)
        result
      }
      case e => throw LynxException("Unknown cache error, value is: "+e.toString)
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

//abstract class CacheInferLinkExecutor extends InferLinkExecutor with CacheInferExecutor {
//  val relType: Long
//
//  def cacheKey(node: LynxNode): CacheKey.Expand = CacheKey.Expand(node.id.toLynxInteger.v, relType)
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
//  private def costInfer(node: LynxNode): (Seq[(LynxRelationship, LynxNode)], Long) = {
//    val t0 = System.currentTimeMillis()
//    val r = _infer(node)
//    (r, System.currentTimeMillis() - t0)
//  }
//
//  def _infer(node: LynxNode): Seq[(LynxRelationship, LynxNode)]
//
//}

abstract class CacheInferPropertyExecutor extends InferPropertyExecutor with CacheInferExecutor {

  def cacheKey(node: LynxNode, prop: LynxPropertyKey): CacheKey.Prop =
    CacheKey.Prop(node.id.toLynxInteger.v, Meta.getCode(prop))

  override def infer(node: LynxNode, props: Seq[LynxPropertyKey]): LynxNode = if(cacheAvailable) {
    // fixme: only head
    val key = cacheKey(node, props.head)
    cache.get(key) match {
      case Some(v: CacheValue.PropValue) => updateNode(node, List((props.head, v.value)))
      case None => {
        val (result, cost) = costInfer(node, props)
        cache.put(key, CacheValue.PropValue(result.property(props.head).get), cost)
        return result
      }
      case e => throw LynxException("Unknown cache error, value is: "+e.toString)
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

  def cacheKey(node: LynxNode): CacheKey.Prop = CacheKey.Prop(node.id.toLynxInteger.v, Meta.LABEL)

  override def infer(node: LynxNode): LynxNode = if(cacheAvailable) {
    val key = cacheKey(node)
    cache.get(key) match {
      case Some(v: CacheValue.PropValue) => updateNode(node, v.value.value.asInstanceOf[String])
      case None => {
        val (result, cost) = costInfer(node)
        cache.put(key, CacheValue.PropValue(LynxString(result.labels.head.value)), cost)
        return result
      }
      case e => throw LynxException("Unknown cache error, value is: "+e.toString)
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
