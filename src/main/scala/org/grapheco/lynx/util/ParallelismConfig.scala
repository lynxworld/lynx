package org.grapheco.lynx.util

/**
 * @Author renhao
 * @Description:
 * @Data 2025/5/26 16:40
 * @Modified By:
 */
object ParallelismConfig {
  private val cpuCores: Int = Runtime.getRuntime.availableProcessors()
  val parallelism: Int = math.max(1, cpuCores - 1)
}
