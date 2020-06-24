package org.opencypher.v9_0.expressions

/**
 * Created by bluejoe on 2020/6/22.
 */
import org.opencypher.v9_0.util.InputPosition
import org.opencypher.v9_0.util.symbols._

trait BlobURL {
  def asCanonicalString: String;
}

case class BlobLiteralExpr(value: BlobURL)(val position: InputPosition) extends Expression {
  override def asCanonicalStringVal = value.asCanonicalString
}

case class BlobFileURL(filePath: String) extends BlobURL {
  override def asCanonicalString = filePath
}

case class BlobBase64URL(base64: String) extends BlobURL {
  override def asCanonicalString = base64
}

case class InternalUrl(blobId: String) extends BlobURL {
  override def asCanonicalString = blobId
}

case class BlobHttpURL(url: String) extends BlobURL {
  override def asCanonicalString = url
}

case class BlobFtpURL(url: String) extends BlobURL {
  override def asCanonicalString = url
}

case class AlgoNameWithThresholdExpr(algorithm: Option[String], threshold: Option[Double])(val position: InputPosition)
  extends Expression {
}

case class CustomPropertyExpr(map: Expression, propertyKey: PropertyKeyName)(val position: InputPosition) extends LogicalProperty {
  override def asCanonicalStringVal = s"${map.asCanonicalStringVal}.${propertyKey.asCanonicalStringVal}"
}

case class SemanticLikeExpr(lhs: Expression, ant: Option[AlgoNameWithThresholdExpr], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = this.getClass.getSimpleName
}

case class SemanticUnlikeExpr(lhs: Expression, ant: Option[AlgoNameWithThresholdExpr], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = this.getClass.getSimpleName
}

case class SemanticCompareExpr(lhs: Expression, ant: Option[AlgoNameWithThresholdExpr], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = this.getClass.getSimpleName
}

case class SemanticSetCompareExpr(lhs: Expression, ant: Option[AlgoNameWithThresholdExpr], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTList(CTList(CTFloat)))
  )

  override def canonicalOperatorSymbol = this.getClass.getSimpleName
}

case class SemanticContainExpr(lhs: Expression, ant: Option[AlgoNameWithThresholdExpr], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = this.getClass.getSimpleName
}

case class SemanticInExpr(lhs: Expression, ant: Option[AlgoNameWithThresholdExpr], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = this.getClass.getSimpleName
}

case class SemanticContainSetExpr(lhs: Expression, ant: Option[AlgoNameWithThresholdExpr], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = this.getClass.getSimpleName
}

case class SemanticSetInExpr(lhs: Expression, ant: Option[AlgoNameWithThresholdExpr], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = this.getClass.getSimpleName
}

