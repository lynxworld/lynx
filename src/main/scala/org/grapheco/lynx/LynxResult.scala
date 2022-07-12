package org.grapheco.lynx

import org.grapheco.lynx.types.LynxValue

trait LynxResult {
  def show(limit: Int = 20): Unit

  def cache(): LynxResult

  def columns(): Seq[String]

  def records(): Iterator[LynxRecord]
}
