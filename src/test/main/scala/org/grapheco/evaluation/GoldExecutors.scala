package org.grapheco.evaluation

import com.github.tototoshi.csv.CSVReader
import org.grapheco.lynx.infer.cache.{CacheKey, InferCache}
import org.grapheco.lynx.infer.{CacheInferExpandExecutor, CacheInferLabelExecutor, CacheInferPropertyExecutor, InferExpandExecutor, InferLabelExecutor, InferLinkExecutor, InferPropertyExecutor}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}

import java.io.File


object GoldExpand extends InferExpandExecutor {

  val data: Map[Int, GoldData.Object] = GoldData.objects

  override def infer(node: LynxNode): Seq[(LynxRelationship, LynxNode)] = {
    Thread.sleep(20)
    node.property(LynxPropertyKey("image_index"))
      .map{case LynxInteger(v) => v.toInt}
      .flatMap(GoldData.contains.get)
      .getOrElse(List.empty)
      .map { out =>
        val outId = TestId(out)
        val e = TestRelationship(
          TestId.nextId,
          node.id.asInstanceOf[TestId],
          outId,
          Some(LynxRelationshipType("contains")),
          Map.empty
        )
        val n = VTNode(outId, Seq.empty, Map.empty, GoldLabel.inferValue)
        (e, n)
      }
  }
}

object GoldProps extends InferPropertyExecutor {

  val data: Map[Int, GoldData.Object] = GoldData.objects

  override def infer(node: LynxNode, props: Seq[LynxPropertyKey] = Seq.empty): LynxNode = {
    Thread.sleep(1)
    val id = node.id.toLynxInteger.value
    val obj = data(id.toInt)
    val n = node.asInstanceOf[VTNode]
    // update node according to props
    props.foldLeft(n) { case (n, prop) =>
      prop.value match {
        case "size" => n.update(Map(prop -> LynxString(obj.size)))
        case "material" => n.update(Map(prop -> LynxString(obj.material)))
        case "color" => n.update(Map(prop -> LynxString(obj.color)))
        case _ => n
      }
    }
  }
}

object GoldLabel extends InferLabelExecutor {
  override def infer(node: LynxNode): LynxNode = node match {
    case v: VTNode => v.update(inferValue(v))
    case _ => node
  }

  def inferValue(node: LynxNode): Seq[LynxNodeLabel] = {
    Thread.sleep(1)
    val id = node.id.toLynxInteger.value
    val obj = GoldData.objects(id.toInt)
    Seq(LynxNodeLabel(obj.shape))
  }
}

object GoldData {

  private def parseIntList(str: String): List[Int] = {
    try {
      str.stripPrefix("[")
        .stripSuffix("]")
        .split(",")
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(_.toInt)
        .toList
    } catch {
      case e: Exception =>
        println(s"Error parsing int list: $str")
        println(s"Error: ${e.getMessage}")
        List.empty[Int]
    }
  }

  /* read csv file in '$projectdir/datasets/contains.csv' as a map
    line :i.image_index,collect (id(o))
          0,"[20495, 20497, 20496, 20498, 20494]"
   */
  val contains: Map[Int, List[Int]] =
    CSVReader.open(new File(s"${System.getProperty("user.dir")}/datasets/contains.csv"))
      .iterator
      .drop(1) //skip header
      .map { line =>
        val Seq(index, ids) = line
        index.toInt -> parseIntList(ids)
      }.toMap

  //  id,size,shape,material,color,behind,front,left,right
  case class Object(id: Int,
                    size: String,
                    shape: String,
                    material: String,
                    color: String,
                    behind: List[Int],
                    front: List[Int],
                    left: List[Int],
                    right: List[Int])

  val objects: Map[Int, Object] =
    CSVReader.open(new File(s"${System.getProperty("user.dir")}/datasets/objects.csv"))
      .iterator
      .drop(1) //skip header
      .map { line =>
        val Seq(id, size, shape, material, color, behind, front, left, right) = line
        id.toInt -> Object(id.toInt, size, shape, material, color, parseIntList(behind), parseIntList(front), parseIntList(left), parseIntList(right))
      }.toMap


}

object GoldLink extends InferLinkExecutor {
  override def infer(node: LynxNode, nodes: Seq[LynxNode]): Seq[(LynxNode, LynxRelationship, LynxNode)] = {
    val id = node.id.toLynxInteger.value
    val obj = GoldData.objects(id.toInt)
    nodes.flatMap { n =>
      val id2 = n.id.toLynxInteger.value
      (obj.behind.contains(id2.toInt) -> "behind" ::
        obj.front.contains(id2.toInt) -> "front" ::
        obj.left.contains(id2.toInt) -> "left" ::
        obj.right.contains(id2.toInt) -> "right" :: Nil)
        .filter(_._1).map { case (_, relType) =>
          (node, TestRelationship(
            TestId.nextId,
            node.id.asInstanceOf[TestId],
            n.id.asInstanceOf[TestId],
            Some(LynxRelationshipType(relType)), Map.empty), n)
        }
    }
  }
}

class GoldExpandCache(implicit inferCache: InferCache) extends CacheInferExpandExecutor {
  override val relType: Long = "contains".hashCode

  override def _infer(node: LynxNode): Seq[(LynxRelationship, LynxNode)] = GoldExpand.infer(node)

  override def cache: InferCache = inferCache
}

class GoldPropsCache(implicit inferCache: InferCache) extends CacheInferPropertyExecutor {
  override def updateNode(node: LynxNode, props: List[(LynxPropertyKey, LynxValue)]): LynxNode = {
    val n = node.asInstanceOf[VTNode]
    props.foldLeft(n) { case (n, prop) => n.update(Map(prop._1 -> prop._2))}
  }

  override def _infer(node: LynxNode, props: Seq[LynxPropertyKey]): LynxNode = GoldProps.infer(node, props)

  override def cache: InferCache = inferCache
}

class GoldLabelCache(implicit inferCache: InferCache) extends CacheInferLabelExecutor {
  override def updateNode(node: LynxNode, label: LynxNodeLabel): LynxNode = {
    node.asInstanceOf[VTNode].update(Seq(label))
  }

  override def _infer(node: LynxNode): LynxNode = GoldLabel.infer(node)

  override def cache: InferCache = inferCache
}