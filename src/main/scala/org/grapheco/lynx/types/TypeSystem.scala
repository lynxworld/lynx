package org.grapheco.lynx.types

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property._
import org.grapheco.lynx.types.time._
import org.opencypher.v9_0.expressions.{BooleanLiteral, DoubleLiteral, IntegerLiteral, StringLiteral}
import org.opencypher.v9_0.util.symbols.{CTAny, CTBoolean, CTFloat, CTInteger, CTList, CTString, CypherType}

import java.time._
import scala.collection.mutable

trait TypeSystem {
  def typeOf(clazz: Class[_]): LynxType

  def wrap(value: Any): LynxValue

  def format(value: LynxValue): String
}

