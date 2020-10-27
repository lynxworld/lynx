package org.opencypher.lynx.planning

sealed trait JoinType

case object InnerJoin extends JoinType
case object LeftOuterJoin extends JoinType
case object RightOuterJoin extends JoinType
case object FullOuterJoin extends JoinType
case object CrossJoin extends JoinType

sealed trait Order

case object  Ascending extends Order
case object  Descending extends Order
