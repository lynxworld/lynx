package demo

import org.grapheco.lynx.KeyValueStore

object DemoKeyValueStoreApp {
  def main(args: Array[String]): Unit = {
    val demoFile = "demo_keyvalue_store.txt"
    val store = new KeyValueStore(demoFile)

    println("=== Demo: KeyValueStore ===")

    // 1. Write key-value pairs
    println("\nWriting key-value pairs...")
    store.append("user1", "Alice")
    store.append("user2", "Bob")

    // 2. Read and display all key-value pairs
    println("\nReading key-value pairs...")
    store.read().foreach { case (key, value) =>
      println(s"$key -> $value")
    }

    // 3. Cache a query result
    println("\nCaching a query result...")
    val query = "MATCH (n) RETURN n LIMIT 10"
    val result = "[{'name':'Alice'}, {'name':'Bob'}]"
    store.cacheQueryResult(query, result)

    // 4. Retrieve cached query result
    println("\nRetrieving cached query result...")
    store.getCachedResult(query) match {
      case Some(res) => println(s"Cached result for query: $res")
      case None      => println("No cached result found.")
    }
  }
}

