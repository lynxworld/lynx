package org.grapheco.evaluation

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import org.grapheco.lynx.LynxResult
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.neo4j.driver.Driver

import java.io.File
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

case class EvalCLEVR(name: String)(implicit database: TestBase, driver: Driver) extends {

  val clevr_1000_small: List[File] = new java.io.File("datasets/CLEVR1000").listFiles().filterNot(_.getName.startsWith(".")).sortBy(_.getName.substring(10,16)).toList

  def initDB(): Unit = {
    if (database.all_nodes.isEmpty) {
      clevr_1000_small.zipWithIndex.map{ case (f,i) =>
        database.all_nodes.put(
          TestId(i),
          TestNode(TestId(i), Seq(LynxNodeLabel("Image")),
            Map(
              LynxPropertyKey("file") -> LynxValue(f.getPath),
              LynxPropertyKey("image_index") -> LynxValue(f.getName.substring(10,16).toInt),
              LynxPropertyKey("id") -> LynxValue(i))
          )
        )
      }
    }
  }

  def query(q: String): EvalResult = {
    initDB()
    val t0 = System.currentTimeMillis()
    val result = database.runner.run(q, Map.empty)
    val r = EvalResult(result.records().map(_.get(0).get.asInstanceOf[LynxInteger].value.toInt))
    val t1 = System.currentTimeMillis()
    println(s"Query time: ${t1 - t0}ms")
    r
  }
}

case class EvalResult(result: Iterator[Int]) {

  def save(file: File): EvalResult = {
    val writer = CSVWriter.open(file)
    val r = result.toList
    r.foreach{ r =>writer.writeRow(Seq(r))}
    writer.close()
    EvalResult(r.iterator)
  }

  def eval(file: File): Metrics = {
    val e = EvalResult.from(file)
    eval(e.result.toList)
  }

  def eval(result: List[Int]): Metrics = Metrics.calculate(this.result.toList, result)

  def eval(cypher: String)(implicit driver: Driver): Metrics = {
    val s = driver.session()
    val labels = s.run(cypher).list().asScala.map(_.get(0).asInt())
    s.close()
    eval(labels.toList)
  }
}

object EvalResult {
  def from(file: File): EvalResult = {
    val reader = CSVReader.open(file)
    val r = EvalResult(reader.all().map(_.head.toInt).iterator)
    reader.close()
    r
  }
}



case class Metrics(accuracy: Double, recall: Double, f1Score: Double) {
  override def toString: String = {
    s"Accuracy: $accuracy, Recall: $recall, F1 Score: $f1Score"
  }

  def show: Metrics = {
    println(this)
    this
  }

  def save(file: File): Metrics = {
    val writer = CSVWriter.open(file)
    writer.writeRow(Seq(accuracy, recall, f1Score))
    writer.close()
    this
  }
}

object Metrics {
  def calculate(preList: List[Int], evalList: List[Int]): Metrics = {
    val predictions = preList.toSet
    val groundTruth = evalList.toSet
    // 计算 TP, FP, FN
    val TP = predictions.intersect(groundTruth).size
    val FP = predictions.diff(groundTruth).size
    val FN = groundTruth.diff(predictions).size

    // 计算准确率
    val accuracy = if (TP + FP > 0) TP.toDouble / (TP + FP) else 0.0

    // 计算召回率
    val recall = if (TP + FN > 0) TP.toDouble / (TP + FN) else 0.0

    // 计算 F1 分数
    val f1Score = if (accuracy + recall > 0)
      2 * (accuracy * recall) / (accuracy + recall)
    else
      0.0

    Metrics(accuracy, recall, f1Score)
  }

}


