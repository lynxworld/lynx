package org.grapheco.lynx.types.time

import org.grapheco.lynx.LynxType
import org.opencypher.v9_0.util.symbols.CTDuration

import java.time.Duration

/**
 * @ClassName LynxDuration
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxDuration(duration: Duration) extends LynxTemporalValue {
  def value: Duration = duration

  def +(that: LynxDuration): LynxDuration = LynxDuration(value.plus(that.value))

  def -(that: LynxDuration): LynxDuration = LynxDuration(value.minus(that.value))

  override def toString: String = {
    val seconds = value.getSeconds
    val nanos = value.getNano
    if (value eq Duration.ZERO) return "PT0S"
    val _hours = seconds / 3600
    val minutes = ((seconds % 3600) / 60).toInt
    val secs = (seconds % 60).toInt
    val years = _hours / 262800
    val months = (_hours % 262800) / 720
    val days = (_hours % 720) / 24
    val hours = _hours % 24
    val buf = new StringBuilder(24)
    buf.append("P")
    if (years != 0) buf.append(years).append('Y')
    if (months != 0) buf.append(months).append('M')
    if (days != 0) buf.append(days).append('D')
    buf.append("T")
    if (hours != 0) buf.append(hours).append('H')
    if (minutes != 0) buf.append(minutes).append('M')
    if (secs == 0 && nanos == 0 && buf.length > 2) return buf.toString
    if (secs < 0 && nanos > 0) if (secs == -1) buf.append("-0")
    else buf.append(secs + 1)
    else buf.append(secs)
    if (nanos > 0) {
      val pos = buf.length
      if (secs < 0) buf.append(2 * 1000000000L - nanos)
      else buf.append(nanos + 1000000000L)
      while ( {
        buf.charAt(buf.length - 1) == '0'
      }) buf.setLength(buf.length - 1)
      buf.setCharAt(pos, '.')
    }
    buf.append('S')
    buf.toString
  }

  def cypherType: LynxType = CTDuration
}
