package demo

import org.grapheco.lynx.KeyValueStore
import scala.util.Try

object DemoKeyValueStoreTest extends App {
  val demoFile = "test_keyvalue_store.txt"
  val store = new KeyValueStore(demoFile)

  // Test writing key-value pairs
  println("Testing append()...")
  assert(store.append("testKey", "testValue").isSuccess)

  // Test reading key-value pairs
  println("Testing read()...")
  val data = store.read()
  assert(data.contains("testKey") && data("testKey") == "testValue")

  // Test caching a query result
  println("Testing cacheQueryResult()...")
  val query = "MATCH (n) RETURN n"
  val result = "[{'name':'Alice'}, {'name':'Bob'}]"
  assert(store.cacheQueryResult(query, result).isSuccess)

  // Test retrieving cached query result
  println("Testing getCachedResult()...")
  assert(store.getCachedResult(query).contains(result))

  println("All tests passed!")
}

