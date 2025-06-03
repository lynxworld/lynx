package org.grapheco.lynx.runner.filter

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.structural.LynxPropertyKey

/**
 * @Author renhao
 * @Description:
 * @Data 2025/5/9 16:51
 * @Modified By:
 */
trait FilterExpr {
  def eval(props: Map[LynxPropertyKey, LynxValue]): Boolean
  def keys(): Seq[LynxPropertyKey]
}

case class Equals(key: LynxPropertyKey, value: LynxValue) extends FilterExpr {
  override def eval(props: Map[LynxPropertyKey, LynxValue]): Boolean = {
    props.get(key).contains(value)
  }

  override def keys(): Seq[LynxPropertyKey] = Seq(key)
}

case class In(key: LynxPropertyKey, values: LynxList) extends FilterExpr {

  override def eval(props: Map[LynxPropertyKey, LynxValue]): Boolean = {
    props.get(key).exists(values.value.contains)
  }
  override def keys(): Seq[LynxPropertyKey] = Seq(key)
}

case class Contains(key: LynxPropertyKey, value: LynxValue) extends FilterExpr {
  override def eval(props: Map[LynxPropertyKey, LynxValue]): Boolean = {
    props.get(key) match {
      case Some(seq: LynxList) => seq.v.contains(value)
      case _ => false
    }
  }
  override def keys(): Seq[LynxPropertyKey] = Seq(key)
}

case class Ands(exprs: Set[FilterExpr]) extends FilterExpr {
  override def eval(props: Map[LynxPropertyKey, LynxValue]): Boolean = {
    exprs.forall(_.eval(props))
  }

  override def keys(): Seq[LynxPropertyKey] = exprs.toSeq.flatMap(_.keys().toSeq)

  def getExprByKey(key: LynxPropertyKey): FilterExpr = mapExpr.get(key).get

  def removeKeys(keys: Seq[LynxPropertyKey]): Ands =
    Ands(mapExpr.toSet.filterNot(kv => keys.contains(kv._1)).map(_._2))

  private val mapExpr: Map[LynxPropertyKey, FilterExpr] = exprs.map(expr => (expr.keys().head, expr)).toMap

}
case class Ors(exprs: Set[FilterExpr]) extends FilterExpr {
  override def eval(props: Map[LynxPropertyKey, LynxValue]): Boolean = {
    exprs.exists(_.eval(props))
  }
  override def keys(): Seq[LynxPropertyKey] = exprs.toSeq.flatMap(_.keys().toSeq)
}
