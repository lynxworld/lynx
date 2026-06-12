package org.grapheco.lynx.procedure

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.LynxString

class FullTextIndexFunctions {
  @LynxProcedure(name = "db.index.fulltext.queryNodes")
  def queryNodes(indexName: LynxString, searchObject: LynxString): String = {
    //TODO:
    ""
  }

  @LynxProcedure(name = "db.index.fulltext.createNodeIndex")
  def queryNodes(titlesAndDescriptions: LynxString, var1: LynxList, var2:LynxList): String = {
    //TODO:
    ""
  }

}
