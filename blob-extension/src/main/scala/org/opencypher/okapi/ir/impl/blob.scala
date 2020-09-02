package org.opencypher.okapi.ir.impl

import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.{expressions => ast}

import scala.collection.mutable.ArrayBuffer

object BlobExprs {
  def convertBlobLiteral(e: ast.ASTBlobLiteral, context: IRBuilderContext): Expr = {
    IRBlobLiteral(BlobFactory.make(e.value, context))
  }

  def convertCustomPropertyExpr(mapExpr: Expr, propertyKey: PropertyKey): Expr = {
    IRCustomProperty(mapExpr, propertyKey)
  }

  def convertSemanticLikeExpr(lhsExpr: Expr, ant: Option[ASTAlgoNameWithThreshold], rhsExpr: Expr): Expr = {
    IRSemanticLike(lhsExpr: Expr, ant: Option[ASTAlgoNameWithThreshold], rhsExpr: Expr)
  }

  def convertSemanticUnlikeExpr(lhsExpr: Expr, ant: Option[ASTAlgoNameWithThreshold], rhsExpr: Expr): Expr = {
    IRSemanticUnlike(lhsExpr: Expr, ant: Option[ASTAlgoNameWithThreshold], rhsExpr: Expr)
  }

  def convertSemanticCompareExpr(lhsExpr: Expr, ant: Option[ASTAlgoNameWithThreshold], rhsExpr: Expr): Expr = {
    IRSemanticCompare(lhsExpr: Expr, ant: Option[ASTAlgoNameWithThreshold], rhsExpr: Expr)
  }

  def convertSemanticSetCompareExpr(lhsExpr: Expr, ant: Option[ASTAlgoNameWithThreshold], rhsExpr: Expr): Expr = {
    IRSemanticSetCompare(lhsExpr: Expr, ant: Option[ASTAlgoNameWithThreshold], rhsExpr: Expr)
  }

  def convertSemanticInExpr(lhsExpr: Expr, ant: Option[ASTAlgoNameWithThreshold], rhsExpr: Expr): Expr = {
    IRSemanticIn(lhsExpr: Expr, ant: Option[ASTAlgoNameWithThreshold], rhsExpr: Expr)
  }

  def convertSemanticContainExpr(lhsExpr: Expr, ant: Option[ASTAlgoNameWithThreshold], rhsExpr: Expr): Expr = {
    IRSemanticContain(lhsExpr: Expr, ant: Option[ASTAlgoNameWithThreshold], rhsExpr: Expr)
  }

  def convertSemanticSetInExpr(lhsExpr: Expr, ant: Option[ASTAlgoNameWithThreshold], rhsExpr: Expr): Expr = {
    IRSemanticSetIn(lhsExpr: Expr, ant: Option[ASTAlgoNameWithThreshold], rhsExpr: Expr)
  }

  def convertSemanticContainSetExpr(lhsExpr: Expr, ant: Option[ASTAlgoNameWithThreshold], rhsExpr: Expr): Expr = {
    IRSemanticContainSet(lhsExpr: Expr, ant: Option[ASTAlgoNameWithThreshold], rhsExpr: Expr)
  }
}

trait Blob {

}

object BlobFactory {
  private val factories = ArrayBuffer[PartialFunction[(BlobURL, IRBuilderContext), Blob]]()

  def configure(x: PartialFunction[(BlobURL, IRBuilderContext), Blob]) = factories += x

  def make(url: BlobURL, context: IRBuilderContext): Blob = {
    factories.find(_.isDefinedAt(url, context)).map(_.apply(url, context)).getOrElse(
      throw new NoSuitableBlobFactory(url)
    )
  }
}

class NoSuitableBlobFactory(url: BlobURL) extends RuntimeException {

}