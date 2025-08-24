package org.grapheco.lynx

import scala.util.{Try, Success, Failure}
import scala.io.Source
import java.io.{BufferedWriter, FileWriter}

class KeyValueStore(val filePath: String) {

  // Read the entire file and parse key-value pairs
  def read(): Map[String, String] = {
    val source = Source.fromFile(filePath)
    try {
      source.getLines()
        .map(_.split("="))
        .filter(_.length == 2)
        .map { case Array(key, value) => key.trim -> value.trim }
        .toMap
    } finally {
      source.close()
    }
  }

  // Write all key-value pairs to the file
  def write(data: Map[String, String]): Try[Unit] = {
    val writer = new BufferedWriter(new FileWriter(filePath))
    try {
      data.foreach { case (key, value) =>
        writer.write(s"$key=$value")
        writer.newLine()
      }
      Success(())
    } catch {
      case ex: Exception => Failure(ex)
    } finally {
      writer.close()
    }
  }
}

object KeyValueStore {
  // Append a single key-value pair to the file
  def writeKeyValue(filePath: String, key: String, value: String): Try[Unit] = {
    val writer = new BufferedWriter(new FileWriter(filePath, true)) // Append mode
    try {
      writer.write(s"$key=$value")
      writer.newLine()
      Success(())
    } catch {
      case ex: Exception => Failure(ex)
    } finally {
      writer.close()
    }
  }

  // Read the file and parse all key-value pairs
  def readKeyValue(filePath: String): Map[String, String] = {
    val source = Source.fromFile(filePath)
    try {
      source.getLines()
        .map(_.split("="))
        .filter(_.length == 2)
        .map { case Array(key, value) => key.trim -> value.trim }
        .toMap
    } finally {
      source.close()
    }
  }
}

