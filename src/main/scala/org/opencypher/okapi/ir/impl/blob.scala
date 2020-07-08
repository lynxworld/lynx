package org.opencypher.okapi.ir.impl

import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.{expressions => ast}

import scala.collection.mutable.ArrayBuffer

object BlobExprs {
  def convertBlobLiteral(e: ast.BlobLiteralExpr, context: IRBuilderContext): Expr = {
    IRBlobLiteral(BlobFactory.make(e.value, context))
  }

  def convertCustomPropertyExpr(mapExpr: Expr, propertyKey: PropertyKey): Expr = {
    IRCustomPropertyExpr(mapExpr, propertyKey)
  }

  def convertSemanticLikeExpr(lhsExpr: Expr, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: Expr): Expr = {
    IRSemanticLikeExpr(lhsExpr: Expr, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: Expr)
  }

  def convertSemanticUnlikeExpr(lhsExpr: Expr, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: Expr): Expr = {
    IRSemanticUnlikeExpr(lhsExpr: Expr, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: Expr)
  }

  def convertSemanticCompareExpr(lhsExpr: Expr, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: Expr): Expr = {
    IRSemanticCompareExpr(lhsExpr: Expr, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: Expr)
  }

  def convertSemanticSetCompareExpr(lhsExpr: Expr, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: Expr): Expr = {
    IRSemanticSetCompareExpr(lhsExpr: Expr, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: Expr)
  }

  def convertSemanticInExpr(lhsExpr: Expr, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: Expr): Expr = {
    IRSemanticInExpr(lhsExpr: Expr, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: Expr)
  }

  def convertSemanticContainExpr(lhsExpr: Expr, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: Expr): Expr = {
    IRSemanticContainExpr(lhsExpr: Expr, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: Expr)
  }

  def convertSemanticSetInExpr(lhsExpr: Expr, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: Expr): Expr = {
    IRSemanticSetInExpr(lhsExpr: Expr, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: Expr)
  }

  def convertSemanticContainSetExpr(lhsExpr: Expr, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: Expr): Expr = {
    IRSemanticContainSetExpr(lhsExpr: Expr, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: Expr)
  }
}

trait Blob {

}

object BlobFactory {
  private val factories = ArrayBuffer[PartialFunction[(BlobURL, IRBuilderContext), Blob]]()

  def add(x: PartialFunction[(BlobURL, IRBuilderContext), Blob]) = factories += x

  def make(url: BlobURL, context: IRBuilderContext): Blob = {
    factories.find(_.isDefinedAt(url, context)).map(_.apply(url, context)).getOrElse(
      throw new NoSuitableBlobFactory(url)
    )
  }
}

class NoSuitableBlobFactory(url: BlobURL) extends RuntimeException {

}