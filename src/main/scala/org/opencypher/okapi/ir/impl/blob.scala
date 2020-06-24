package org.opencypher.okapi.ir.impl

import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.{expressions => ast}

object BlobExprs {
  def convertBlobLiteral(e: ast.BlobLiteralExpr): Expr = {
    IRBlobLiteral(e.value)
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

