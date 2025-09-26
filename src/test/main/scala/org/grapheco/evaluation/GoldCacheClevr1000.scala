package org.grapheco.evaluation

import com.github.tototoshi.csv.CSVWriter
import org.grapheco.lynx.infer.cache.{CacheKey, CacheValue}
import org.grapheco.lynx.infer.cache.core.CachePolicy
import org.grapheco.lynx.infer.cache.policy.{LDCECache, LFUCache, LRUCache, RandomCache}
import org.junit.jupiter.api.{DynamicTest, Test, TestFactory}

import scala.collection.JavaConverters.asJavaCollectionConverter
import scala.util.Random

class GoldCacheClevr1000 extends Clevr1000 {
  val CAP =  100
  val cache = new LDCECache[CacheKey, CacheValue](CAP, candidateK = 10, Wc = 1, Wf = 1, Wt = 1)
//  val cache = new LRUCache[CacheKey, CacheValue](CAP)
//  val cache = new LFUCache[CacheKey, CacheValue](CAP)
//  val cache = new RandomCache[CacheKey, CacheValue](CAP)
  override implicit val db: GoldVirtualCacheTestBase = new GoldVirtualCacheTestBase(cache)

  @TestFactory
  def goldTest(): java.util.Collection[DynamicTest] = {
    initDB(100)
    questions.take(100)
      .sortBy(_.template_filename)
      .map{ question =>DynamicTest.dynamicTest(question.id, question.executable(profile = false))}
      .asJavaCollection
  }

  @TestFactory
  def goldTemplateTest(): java.util.Collection[DynamicTest] = {
    initDB(100)
    val templateName = "compare_integer.json"
    questions.take(1000)
      .filter(_.template_filename == templateName)
      .sortBy(_.template_filename)
      .map{ question => DynamicTest.dynamicTest(question.id, question.executable(profile = true))}
      .asJavaCollection
  }

//  @Test
//  def zipfTest(): Unit = {
//    val requests = ZipfWorkloadBuilder.buildZipfImageLevel(numImages = 1000, totalRequests = 10000)
//    requests
//  }
  def zipfQuestions(numImages: Int, totalRequests: Int, theta: Double = 0.9): List[ClevrQuestion] = {
    val qmap = questions.map(q => q.question_index -> q).toMap
    ZipfWorkloadBuilder.buildZipfImageLevel(numImages = numImages, totalRequests = totalRequests, theta = theta)
      .map(_.key).map(k => qmap(k._1 * 10 + k._2)).toList
  }

  @Test
  override def evaluation(): Unit = {
//    val out = "eval_questions_local_device.csv"
//    val writer = CSVWriter.open(out)
    val writer = null
    var num = 0
    val all = 1000
    initDB(1000)
    val time0 = System.currentTimeMillis()
    val shuffled = zipfQuestions(numImages = 100, totalRequests = 1000)
    shuffled.foreach { question =>
      question.eval(Option(writer))
      num += 1
      if (num % 100 == 0) println(s"[$num/$all]")
    }
    println(s"time taken ${System.currentTimeMillis() - time0} ms")
    println(db._inferCache.metrics)
  }

  def cacheEvaluationRunner(
                             cacheName: String,
                             capacity: Int,
                             kNumber: Int,
                             W: (Int, Int, Int),
                             theta: Double,
                           ): String = {
    val cache: CachePolicy[CacheKey, CacheValue] = cacheName match {
      case "LRU" => new LRUCache[CacheKey, CacheValue](capacity)
      case "LFU" => new LFUCache[CacheKey, CacheValue](capacity)
      case "Random" => new RandomCache[CacheKey, CacheValue](capacity)
      case "LDCE" => new LDCECache[CacheKey, CacheValue](capacity, candidateK = kNumber, Wc = W._1, Wf = W._2, Wt = W._3)
    }
    val db = new GoldVirtualCacheTestBase(cache)
    val out = s"${cache.name}-${capacity}-${theta}.csv"
    val writer = CSVWriter.open(out)
    var num = 0
    val all = 1000
    initDB(1000)(db)
    val time0 = System.currentTimeMillis()
    val shuffled = zipfQuestions(numImages = 100, totalRequests = 1000, theta = theta)
    shuffled.foreach { question =>
      question.eval(Option(writer))(db)
      num += 1
      if (num % 100 == 0) println(s"[$num/$all]")
    }
    val m = db._inferCache.metrics
    s"${cache.name},${capacity},${theta},${System.currentTimeMillis() - time0}, ${m.hitRatio}, ${m.totalReq}, ${m.gets}, ${m.hits}, ${m.recomputeCost}"
  }

  @Test
  def cacheEvaluation(): Unit = {
    val theta = List(0.9, 0.8, 0.7, 0.6, 0.5)
    val capacity = List(100, 200, 300, 400, 500)
    val caches = List("LRU", "LFU", "Random", "LDCE")
    val tests = for {
      cache <- caches
      cap <- capacity
      t <- theta
    } yield (cache, cap, t)
    val writer = CSVWriter.open("cache_evaluation.csv")
    writer.writeRow("cache_name,capacity,theta,time,hitRatio,totalReq,gets,hits,recomputeCost")
    tests.foreach { case (cache, cap, t) =>
      val result = cacheEvaluationRunner(cache, cap, kNumber = cap/10, W = (1, 1, 1), theta = t)
      writer.writeRow(result)
    }
    writer.close()

  }

  @Test
  def compare_integer(): Unit = {
    initDB(10)
    db.runner.run(
      """
        |MATCH (i:Image{image_index: 7})~[:contains]~~<objects>
        | WITH collect(objects) AS objects
        | OPTIONAL  MATCH <o:sphere>~[:behind]~~<o2{color:'brown',material:'rubber'}>
        | WHERE o IN objects AND o2 IN objects
        | OPTIONAL  MATCH <o3:cube{size:'large',material:'rubber'}>
        | WHERE o3 IN objects
        | RETURN count(DISTINCT o2)<count(DISTINCT o3)
        |""".stripMargin, Map.empty, profile = true).show()
  }



  @Test
  def compare(): Unit = {
    initDB(10)
    db.runner.run(
      """
        |MATCH (i:Image{image_index: 6})~[:contains]~~<objects> WITH i,collect(objects) AS objects
        | MATCH <o{material:'metal'}>~[:front]~~<o2:cube>
        | WHERE o IN objects AND o2 IN objects
        | RETURN labels(o2)
        |""".stripMargin, Map.empty, profile = true).show()
  }

  @Test
  def single_or(): Unit = singleRun {
    """
      |MATCH (i:Image{image_index: 71})~[:contains]~~<objects:cube> WITH i,collect(objects) AS objects OPTIONAL  MATCH <o{color:'red',material:'rubber'}> WHERE o IN objects WITH objects,o OPTIONAL  MATCH <o2{color:'red'}> WHERE o2 IN objects WITH collect( distinct o) + collect(distinct o2) AS all unwind all as a
      |RETURN count( distinct a)
      |
      |""".stripMargin
  }

  @Test
  def same_relate(): Unit = singleRun {
    """
      |MATCH (i:Image{image_index: 54})~[r:contains]~~<o2>,(i)~[r2:contains]~~<o:sphere{material:'metal'}>
      |WHERE o.color=o2.color
      |return o,o2,r,r2
      |
      |""".stripMargin
  }

  @Test
  def temp2(): Unit = {
    singleRun(
      """
        |MATCH (i:Image{image_index: 9})~[:contains]~~<objects>
        | WITH collect(objects) AS objects
        | OPTIONAL  MATCH <o{size:'large',color:'gray'}>~[:front]~~<o2{size:'small',color:'gray',material:'rubber'}>
        | WHERE o IN objects AND o2 IN objects
        | OPTIONAL  MATCH <o3{size:'small',color:'brown'}>~[:left]~~<o4>
        | WHERE o3 IN objects AND o4 IN objects
        | RETURN count(DISTINCT o2)=count(DISTINCT o4)
        |
        |
        |""".stripMargin)
  }

  @Test
  def oneHop(): Unit = singleRun {
    """
      |MATCH (i:Image{image_index: 4})~[:contains]~~<objects>
      |WITH i,collect(objects) AS objects
      |MATCH <o{color:'purple'}>~[:front]~~<o2{size:'small'}>
      |WHERE o IN objects AND o2 IN objects
      |RETURN o2.color
      |""".stripMargin
  }

  override def singleRun(query: String, init: Int, profile: Boolean = true): Unit = {
    super.singleRun(query, init)
    println(db._inferCache.metrics)
  }
}
