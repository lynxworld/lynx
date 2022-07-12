package org.grapheco.lynx

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxBoolean, LynxFloat, LynxInteger, LynxNull, LynxString}

case class LynxRecord(cols: Map[String, Int], values: Seq[LynxValue]){
  def apply(columnName: String): LynxValue = get(columnName).getOrElse(LynxNull)

  def get(columnName: String): Option[LynxValue] = cols.get(columnName).map(values.apply)

  def get(columnIndex: Int): Option[LynxValue] = values.lift(columnIndex)

  def getAsString(columnName: String): Option[LynxString] = get(columnName).map(_.asInstanceOf[LynxString])

  def getAsInt(columnName: String): Option[LynxInteger] = get(columnName).map(_.asInstanceOf[LynxInteger])

  def getAsDouble(columnName: String): Option[LynxFloat] = get(columnName).map(_.asInstanceOf[LynxFloat])

  def getAsBoolean(columnName: String): Option[LynxBoolean] = get(columnName).map(_.asInstanceOf[LynxBoolean])

  def toMap: Map[String, LynxValue] = cols.keys.zip(values).toMap
}
