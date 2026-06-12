package org.grapheco.lynx.physical.planner.translators
import org.grapheco.lynx.types.structural.LynxNodeLabel

// MetaData needs to be obtained after cardinality estimation
object MetaData {
  val nodeNum: Int = Int.MaxValue
  val labelNum:Map[LynxNodeLabel,Int]=Map(
    LynxNodeLabel("Person")->100
  )
  //val indexSeq:Set[(LynxNodeLabel,PropertyKeyName)]=Set((LynxNodeLabel("Person"),"name"))
  val indexSeq:Set[(LynxNodeLabel,String)]=Set(
    (LynxNodeLabel("Person"),"lastName")
  )
  val propertiesTypeNum:Map[LynxNodeLabel, Map[String, Int]]=Map(
    LynxNodeLabel("Person")->Map("lastName"->100,"gender"->2)
  )
}