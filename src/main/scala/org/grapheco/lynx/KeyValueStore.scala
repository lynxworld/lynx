package org.grapheco.lynx

import scala.util.{Try, Success, Failure}
import scala.io.Source
import java.io.{BufferedWriter, FileWriter}

/**
 * KeyValueStore: A utility class for managing key-value pairs in a file.
 * Extended to support caching Lynx database query results.
 */
class KeyValueStore(val filePath: String) {

  // Read all key-value pairs from the file
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

  // Append a single key-value pair to the file
  def append(key: String, value: String): Try[Unit] = {
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

  // Cache a query result with a unique key
  def cacheQueryResult(query: String, result: String): Try[Unit] = {
    append(query.hashCode.toString, result)
  }

  // Retrieve cached query result by query string
  def getCachedResult(query: String): Option[String] = {
    val data = read()
    data.get(query.hashCode.toString)
  }
}

object KeyValueStore {
  // Utility to quickly write a key-value pair
  def writeKeyValue(filePath: String, key: String, value: String): Try[Unit] = {
    val store = new KeyValueStore(filePath)
    store.append(key, value)
  }

  // Utility to quickly read all key-value pairs
  def readKeyValue(filePath: String): Map[String, String] = {
    val store = new KeyValueStore(filePath)
    store.read()
  }
}

