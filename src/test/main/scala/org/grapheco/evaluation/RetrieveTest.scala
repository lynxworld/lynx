package org.grapheco.evaluation

import com.github.tototoshi.csv.CSVWriter
import org.junit.jupiter.api.{BeforeAll, DynamicTest, Test, TestFactory}

@Test
class RetrieveTest extends Clevr1000 {

  val retrieveQueries: Map[String, String] = Map(
    "color" -> """
        |MATCH (i:Image)~[:contains]~~<o{color:'yellow'}>
        |WHERE i.image_index >=0 AND i.image_index < 100
        |RETURN DISTINCT i.image_index AS id
        |""".stripMargin,
    "count_between" -> """
       |MATCH (i:Image)~[:contains]~~<objects>
       |WHERE i.image_index >=100 AND i.image_index < 200
       |WITH i.image_index as id, count(objects) as count
       |WHERE count > 4 AND count < 8
       |RETURN id
       |""".stripMargin,
    "count_cube" -> """
        |MATCH (i:Image)~[:contains]~~<cubes:cube>
        |WHERE i.image_index >=200 AND i.image_index < 300
        |WITH i.image_index as id, count(cubes) as count
        |WHERE count = 3
        |RETURN id
        |""".stripMargin,
    "count_red" -> """
        |MATCH (i:Image)~[:contains]~~<objects{color:'red'}>
        |WHERE i.image_index >=300 AND i.image_index < 400
        |WITH i.image_index as id, count(objects) as count
        |WHERE count > 2
        |RETURN id
        |""".stripMargin,
    "count_size" -> """
        |MATCH (i:Image)~[:contains]~~<o>
        |WHERE i.image_index >=400 AND i.image_index < 500
        |WITH i,
        |     SUM(CASE WHEN o.size = 'large' THEN 1 ELSE 0 END) AS largeCount,
        |     SUM(CASE WHEN o.size = 'small' THEN 1 ELSE 0 END) AS smallCount
        |WHERE largeCount < 5 AND smallCount > 2
        |RETURN i.image_index AS id
        |""".stripMargin,
    "large_more" -> """
        |MATCH (i:Image)~[:contains]~~<o>
        |WHERE i.image_index >=500 AND i.image_index < 600
        |WITH i,
        |     SUM(CASE WHEN o.size = 'large' THEN 1 ELSE 0 END) AS largeCount,
        |     SUM(CASE WHEN o.size = 'small' THEN 1 ELSE 0 END) AS smallCount
        |WHERE largeCount > smallCount
        |RETURN i.image_index AS id
        |""".stripMargin,
    "color_material" -> """
        |MATCH (i:Image)~[:contains]~~<o{color:'red', material:'metal'}>
        |WHERE i.image_index >=600 AND i.image_index < 700
        |RETURN DISTINCT i.image_index AS id
        |""".stripMargin,
    "color_material_size" -> """
        |MATCH (i:Image)~[:contains]~~<o{color:'blue', material:'rubber', size: 'small'}>
        |WHERE i.image_index >=700 AND i.image_index < 800
        |RETURN DISTINCT i.image_index AS id
        |""".stripMargin,
    "color_material_size_shape" -> """
        |MATCH (i:Image)~[:contains]~~<o:sphere{color:'purple', material:'metal', size: 'large'}>
        |WHERE i.image_index >=800 AND i.image_index < 900
        |RETURN DISTINCT i.image_index AS id
        |""".stripMargin,
    "color_material_size_shape_count" -> """
        |MATCH (i:Image)~[:contains]~~<o:cylinder{material:'rubber', size: 'small'}>
        |WHERE i.image_index >=900 AND i.image_index < 1000 AND o.color in ['gray', 'blue', 'green']
        |WITH i.image_index as id, count(o) as count
        |WHERE count > 1
        |RETURN DISTINCT id
        |""".stripMargin,
  )

  val aggregationQueries: Map[String, String] = Map(
    "count_all" -> """
       |MATCH (i:Image)~[:contains]~~<o>
       |WHERE i.image_index >=0 AND i.image_index < 200
       |WITH i.image_index as id, COUNT(o) AS objectCount
       |RETURN toInteger(id / 10) AS GroupID, toInteger(SUM(objectCount)) AS result
       |ORDER BY GroupID
       |""".stripMargin,
    "count_size" -> """
       |MATCH (i:Image)~[:contains]~~<o{size: 'small'}>
       |WHERE i.image_index >=200 AND i.image_index < 400
       |WITH i.image_index as id, COUNT(o) AS objectCount
       |RETURN toInteger(id / 10) AS GroupID, toInteger(SUM(objectCount)) AS result
       |ORDER BY GroupID
       |""".stripMargin,
    "count_color" -> """
       |MATCH (i:Image)~[:contains]~~<o{color: 'red'}>
       |WHERE i.image_index >=400 AND i.image_index < 600
       |WITH i.image_index as id, COUNT(o) AS objectCount
       |RETURN toInteger(id / 10) AS GroupID, toInteger(SUM(objectCount)) AS result
       |ORDER BY GroupID
       |""".stripMargin,
    "count_material" -> """
       |MATCH (i:Image)~[:contains]~~<o{material: 'metal'}>
       |WHERE i.image_index >=600 AND i.image_index < 800
       |WITH i.image_index as id, COUNT(o) AS objectCount
       |RETURN toInteger(id / 10) AS GroupID, toInteger(SUM(objectCount)) AS result
       |ORDER BY GroupID
       |""".stripMargin,
    "count_size_comp" -> """
       |MATCH (i:Image)~[:contains]~~<o>
       |WHERE i.image_index >=800 AND i.image_index < 1000
       |WITH i.image_index AS id,
       |     SUM(CASE WHEN o.size = 'large' THEN 1 ELSE 0 END) AS largeCount,
       |     SUM(CASE WHEN o.size = 'small' THEN 1 ELSE 0 END) AS smallCount
       |RETURN toInteger(id / 10) AS GroupID, toInteger(SUM(largeCount)) - toInteger(SUM(smallCount)) AS result
       |ORDER BY GroupID
       |""".stripMargin
  )

  @Test
  def retrieveGoldTest(): Unit = {
    val gold:GoldVirtualTestBase = new GoldVirtualTestBase()
    initDB(1000)(gold)
    val out = s"retrieve_result/gold.csv"
    val writer = CSVWriter.open(out)
    writer.writeRow(Seq("test", "id")) //header

    retrieveQueries.foreach { case (name, question) =>
      val res = gold.runner.run(question, Map.empty, profile = true)
        .records().flatMap(_.getAsInt("id")).toList.map(_.value)
      writer.writeRow(Seq(name, res.mkString("[",",","]")))
    }
    writer.close()
  }

  @Test
  def aggregationGoldTest(): Unit = {
    val gold:GoldVirtualTestBase = new GoldVirtualTestBase()
    initDB(1000)(gold)
    val out = s"aggregation_result/gold.csv"
    val writer = CSVWriter.open(out)
    writer.writeRow(Seq("test", "count")) //header

    aggregationQueries.foreach { case (name, question) =>
      val res = gold.runner.run(question, Map.empty, profile = true)
        .records().flatMap(_.getAsInt("result")).toList.map(_.value)
      writer.writeRow(Seq(name, res.mkString("[",",","]")))
    }
    writer.close()
  }

  @Test
  def retrieveTest(): Unit = {
    initDB(1000)
    val out = s"retrieve_result/retrieve.csv"
    val writer = CSVWriter.open(out)
    writer.writeRow(Seq("test", "result", "time")) //header

    retrieveQueries.foreach { case (name, question) =>
      val t0 = System.nanoTime()
      val res = db.runner.run(question, Map.empty, profile = false)
        .records().flatMap(_.getAsInt("id")).toList.map(_.value)
      val time = System.nanoTime() - t0
      writer.writeRow(Seq(name, res.mkString("[",",","]"), time))
    }
    writer.close()
  }

  @Test
  def aggregationTest(): Unit = {
    initDB(1000)
    val out = s"aggregation_result/aggregation.csv"
    val writer = CSVWriter.open(out)
    writer.writeRow(Seq("test", "count", "time")) //header

    aggregationQueries.foreach { case (name, question) =>
      val t0 = System.nanoTime()
      val res = db.runner.run(question, Map.empty, profile = false)
        .records().flatMap(_.getAsInt("result")).toList.map(_.value)
      val time = System.nanoTime() - t0
      writer.writeRow(Seq(name, res.mkString("[",",","]"), time))
    }
    writer.close()
  }

  @Test
  def test10(): Unit = {
    val gold:GoldVirtualTestBase = new GoldVirtualTestBase()
    initDB(1000)(gold)
    gold.runner.run("""
      |MATCH (i:Image)~[:contains]~~<o>
      |WITH i.image_index as id, COUNT(o) AS objectCount
      |RETURN toInteger(id / 10) AS GroupID, SUM(objectCount) AS result
      |ORDER BY GroupID
      |""".stripMargin, Map.empty).show()
  }

  @Test
  def one(): Unit = {
    initDB()
    db.runner.run(
      """
        |MATCH (i:Image)~[:contains]~~<objects>
        |WHERE i.image_index >=0 AND i.image_index < 10
        |WITH i, objects, collect(objects) AS obs
        |OPTIONAL MATCH <objects>~[:right]~~<o2>
        |WHERE o2 IN obs
        |WITH i.image_index as id, o1, count(o2) as counto2
        |where counto2=0
        |RETURN id, o1.color, counto2""".stripMargin, Map.empty, profile=true).show()
  }

}

