package org.grapheco.lynx

import java.io.File

object KeyValueStoreTest extends App {
  val testFile = "test_kv_store.txt"

  // Write key-value pairs
  KeyValueStore.writeKeyValue(testFile, "key1", "value1")
  KeyValueStore.writeKeyValue(testFile, "key2", "value2")

  // Read key-value pairs and print them
  KeyValueStore.readKeyValue(testFile) match {
    case kvPairs: Map[String, String] =>
      println("Key-Value pairs:")
      kvPairs.foreach { case (key, value) => println(s"$key -> $value") }
  }

  // Cleanup: delete the test file after test
  new File(testFile).delete()
}

