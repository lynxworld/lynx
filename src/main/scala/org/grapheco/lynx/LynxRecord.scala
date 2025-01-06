package org.grapheco.lynx

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxBoolean, LynxFloat, LynxInteger, LynxNull, LynxString}
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger(classOf[LynxRecord])

case class LynxRecord(cols: Map[String, Int], values: Seq[LynxValue]){
  def apply(columnName: String): LynxValue = get(columnName).getOrElse(LynxNull)

 def get(columnName: String): Option[LynxValue] = {
  val result = cols.get(columnName).map(values.apply)
  if (result.isEmpty) logger.debug(s"Column '$columnName' not found.")
  result
}

  def get(columnIndex: Int): Option[LynxValue] = values.lift(columnIndex)

def getAsString(columnName: String): Option[LynxString] = get(columnName) match {
  case Some(value: LynxString) => Some(value)
  case _ => None
}

def getAsDate(columnName: String): Option[LynxDate] = get(columnName) match {
  case Some(value: LynxDate) => Some(value)
  case _ => None
}

def getAsList(columnName: String): Option[Seq[LynxValue]] = get(columnName) match {
  case Some(value: LynxList) => Some(value.elements)
  case _ => None
}

def getAsInt(columnName: String): Option[LynxInteger] = get(columnName) match {
  case Some(value: LynxInteger) => Some(value)
  case _ => None
}

  def getAsDouble(columnName: String): Option[LynxFloat] = get(columnName)  match {
  case Some(value: LynxFloat) => Some(value)
  case _ => None
}

  def getAsBoolean(columnName: String): Option[LynxBoolean] = get(columnName)  match {
  case Some(value:LynxBoolean) => Some(value)
  case _ => None
}

  def getAsNode(columnName: String): Option[LynxNode] = get(columnName)  match {
  case Some(value:LynxNode) => Some(value)
  case _ => None
}

  def getAsRelationship(columnName: String): Option[LynxRelationship] = get(columnName)  match {
  case Some(value:LynxRelationship) => Some(value)
  case _ => None
}

def getIgnoreCase(columnName: String): Option[LynxValue] = {
  cols.find { case (key, _) => key.equalsIgnoreCase(columnName) }
      .flatMap { case (_, index) => values.lift(index) }
}

  def toMap: Map[String, LynxValue] = cols.map { case (key, index) =>
  key -> values.lift(index).getOrElse(LynxNull)
}
}
