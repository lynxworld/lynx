package org.grapheco.lynx.parser

import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.ast.semantics.SemanticState

import scala.collection.mutable

/**
 * @ClassName CachedQueryParser
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
class CachedQueryParser(parser: QueryParser) extends QueryParser {
  val cache = mutable.Map[String, (Statement, Map[String, Any], SemanticState)]()

  override def parse(query: String): (Statement, Map[String, Any], SemanticState) =
    cache.getOrElseUpdate(query, parser.parse(query))
}
