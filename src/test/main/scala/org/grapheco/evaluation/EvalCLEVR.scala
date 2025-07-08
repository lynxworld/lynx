package org.grapheco.evaluation

import com.github.tototoshi.csv.CSVWriter
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

  case class EvalResult(result: LynxResult)(implicit driver: Driver) {
//    def from(file: File): EvalResult = {
//
//    }

    def save(file: File): EvalResult = {
      val writer = CSVWriter.open(file)
      writer.writeRow(result.columns())
      writer.writeAll(result.records().map(_.values).toSeq)
      writer.close()
      this
    }

    def eval(cypher: String): Metrics = {
      val s = driver.session()
      val labels = s.run(cypher).list().asScala.map(_.get(0).asInt())
      val metrics = Metrics.calculate(result.records().map(_.get(0).get.asInstanceOf[LynxInteger].value.toInt).toList, labels.toList)
      s.close()
      println(metrics)
      metrics
    }
  }

  def query(q: String): EvalResult = {
    initDB()
    val result = database.runOnDemoGraph(q)
    EvalResult(result)
  }
}


case class Metrics(accuracy: Double, recall: Double, f1Score: Double) {
  override def toString: String = {
    s"Accuracy: $accuracy, Recall: $recall, F1 Score: $f1Score"
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


