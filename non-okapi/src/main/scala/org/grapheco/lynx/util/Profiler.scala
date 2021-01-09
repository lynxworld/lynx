package org.grapheco.lynx.util

object Profiler {
  var enableTiming = false;

  def timing[T](runnable: => T): T = if (enableTiming) {
    val t1 = System.nanoTime()
    var result: T = null.asInstanceOf[T];
    result = runnable

    val t2 = System.nanoTime()

    println(new Exception().getStackTrace()(1).toString)

    val elapsed = t2 - t1;
    if (elapsed > 1000000) {
      println(s"time cost: ${elapsed / 1000000}ms")
    }
    else {
      println(s"time cost: ${elapsed / 1000}us")
    }

    result
  }
  else {
    runnable
  }
}
