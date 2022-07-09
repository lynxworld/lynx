package org.grapheco.lynx.util

object Profiler {
  var enableTiming = true;

  def timing[T](message: String = "", runnable: => T): T = if (enableTiming) {
    val t1 = System.currentTimeMillis()
    var result: T = null.asInstanceOf[T];
    result = runnable

    val t2 = System.currentTimeMillis()

    println(new Exception().getStackTrace()(1).toString)

    val elapsed = t2 - t1;
    println(s"$message time cost: ${elapsed}ms")

    result
  }
  else {
    runnable
  }
}
