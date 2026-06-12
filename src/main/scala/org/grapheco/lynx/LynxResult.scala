package org.grapheco.lynx

trait LynxResult {
  def show(limit: Int = 20): Unit

  def cache(): LynxResult

  def columns(): Seq[String]

  def records(): Iterator[LynxRecord]

  def asString(): String
}