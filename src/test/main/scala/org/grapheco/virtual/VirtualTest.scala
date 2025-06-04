package org.grapheco.virtual

import org.junit.jupiter.api.{BeforeAll, BeforeEach, Test}

class VirtualTest extends VirtualTestBase {

  val imgsDir = "test-imgs/"

  @BeforeEach
  def init(): Unit = {

    runOnDemoGraph(
      """
//        |CREATE (:Image {name: "Image1", file: "test-imgs/test.jpg"})
//        |CREATE (:Image {name: "Image2", file: "test-imgs/test2.jpg"})
//        |CREATE (:Image {name: "Image3", file: "test-imgs/test3.jpg"})
        |CREATE (:Image {name: "Image3", file: "test-imgs/CLEVR_val_000000.png"})
        |""".stripMargin)
  }


  @Test
  def test(): Unit = {
    runOnDemoGraph(
      """
        |MATCH <c:cat>~[:on]~~<:bed>~~[:on]~<dog>
        |RETURN n, c
        |""".stripMargin)
  }

  "Description(NL) => Image"
  "Query => Image"

  @Test
  def test2(): Unit = {
    runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n, c
        |""".stripMargin)
  }
}
