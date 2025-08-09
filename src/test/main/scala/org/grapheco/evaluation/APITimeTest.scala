package org.grapheco.evaluation

import org.junit.jupiter.api.Test
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase}

object APITimeTest {
  implicit val neo4j: Driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123"))

  def main(args: Array[String]): Unit = {

  }
}
