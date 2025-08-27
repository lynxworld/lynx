package org.grapheco.evaluation

import com.github.tototoshi.csv.CSVReader
import org.grapheco.lynx.LynxException
import org.grapheco.lynx.runner.infer.{InferExpandExecutor, InferLabelExecutor, InferLinkExecutor, InferPropertyExecutor}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxInteger, LynxNumber, LynxString}
import org.grapheco.lynx.types.structural._
import org.json4s.DefaultReaders.StringReader
import org.json4s.native.JsonMethods
import org.json4s.{DefaultFormats, Formats}
import sttp.client4.httpurlconnection.HttpURLConnectionBackend
import sttp.client4.{SyncBackend, UriContext, basicRequest, multipartFile}

import java.awt.image.BufferedImage
import java.awt.{Color, Rectangle}
import java.io.File
import javax.imageio.ImageIO
import scala.util.Random
import scala.util.Random.javaRandomToRandom


object ContainsInfer extends InferExpandExecutor {
  // 创建一个基本的 HTTP 客户端
  implicit val backend: SyncBackend = HttpURLConnectionBackend()
  implicit val formats: Formats = DefaultFormats

  val extractUrl = uri"http://10.0.82.200:52109/segment"

  val size_threshold = 14.29

  case class Mask(area: Int, bbox: List[Int], predicted_iou: Double, segmentation: List[List[Int]])

  override def infer(node: LynxNode): Seq[(LynxRelationship, LynxNode)] = {
    val time0 = System.currentTimeMillis()
    val file: File = node.property(LynxPropertyKey("file")).map {
      case v: LynxString => new File(v.value)
      case _ => throw new RuntimeException("file property is not a string")
    }.getOrElse(throw new RuntimeException("file property not found"))
    val extractRequest = basicRequest
      .post(extractUrl)
      .multipartBody(multipartFile("image", file))

    val time1 = System.currentTimeMillis()
    // 发送请求并获取响应
    val extractResponse = extractRequest.send(backend)
    extractResponse.body match {
      case Right(response) => {
        val time2 = System.currentTimeMillis()
        val j = JsonMethods.parse(response)
        val result = j.extract[List[Mask]].map { mask =>
          val outId = TestId.nextId
          val e = TestRelationship(
            TestId.nextId,
            node.id.asInstanceOf[TestId],
            outId,
            Some(LynxRelationshipType("contains")),
            Map.empty
          )
          val str = s"cache/${System.currentTimeMillis()}.png"

          def size(mask: Mask): Boolean = {
            val centerY = mask.bbox(1) + mask.bbox(3) / 2
            mask.area / centerY > size_threshold
          }

          val n = VTNode(
            outId,
            Seq(),
            Map(
              LynxPropertyKey("file") -> LynxString(str),
              LynxPropertyKey("area") -> LynxInteger(mask.area),
              LynxPropertyKey("size") -> LynxString(if(size(mask))"large" else "small"),
              LynxPropertyKey("box") -> LynxList(mask.bbox.map(LynxValue.apply))
            ),
            getLabels = ShapeInfer.inferValue
          )
          cropImage(file.getAbsolutePath, mask.bbox, mask.segmentation, str)

          (e, n)
        }
        val time3 = System.currentTimeMillis()
        println(s"${time1-time0}, ${time2-time1}, ${time3-time2}")
        result
      }
      case Left(error) => throw new RuntimeException(s"Error: $error")
    }
  }

  def cropImage(imagePath: String, box: List[Int], mask: List[List[Int]], savePath: String): Unit = {
    // 加载原图
    val originalImage: BufferedImage = ImageIO.read(new File(imagePath))

    // 切割图像
    val croppedImage = originalImage.getSubimage(box(0), box(1), box(2), box(3))
    val outputFile = new File(savePath)

    if (mask != List.empty) {
      // 创建一个新的图像用于保存遮罩效果
      val maskedImage = new BufferedImage(croppedImage.getWidth, croppedImage.getHeight, BufferedImage.TYPE_INT_ARGB)

      // 遍历 mask，进行遮罩处理
      for (y <- mask.indices; x <- mask(y).indices) {
        if (mask(y)(x) == 1) {
          // 如果 mask 中的值为 1，保留原图像素
          maskedImage.setRGB(x, y, croppedImage.getRGB(x, y))
        } else {
          // 如果 mask 中的值为 0，设置为透明
          maskedImage.setRGB(x, y, new Color(0, 0, 0, 0).getRGB)
        }
      }
      ImageIO.write(maskedImage, "png", outputFile)
    } else {
      ImageIO.write(croppedImage, "png", outputFile)
    }
  }
}

case class LabelScore(label: String, score: Double)

object ColorInfer extends InferPropertyExecutor {
  // 创建一个基本的 HTTP 客户端
  implicit val backend: SyncBackend = HttpURLConnectionBackend()
  implicit val formats: Formats = DefaultFormats

  val extractUrl = uri"http://10.0.82.200:52109/color"

  override def infer(node: LynxNode, props: Seq[LynxPropertyKey] = Seq.empty): LynxNode = {
    val file: File = node.property(LynxPropertyKey("file")).map {
      case v: LynxString => new File(v.value)
      case _ => throw new RuntimeException("file property is not a string")
    }.getOrElse(throw new RuntimeException("file property not found"))

    val extractRequest = basicRequest
      .post(extractUrl)
      .multipartBody(multipartFile("image", file))

    // 发送请求并获取响应
    val extractResponse = extractRequest.send(backend)
    extractResponse.body match {
      case Right(response) => {
        val j = JsonMethods.parse(response)
        val label = (j \ "label").as[String]
        val n = node.asInstanceOf[VTNode]
        n.update(Map(LynxPropertyKey("color") -> LynxString(label)))
      }
      case Left(error) => throw new RuntimeException(s"Error: $error")
    }
  }
}

object MaterialInfer extends InferPropertyExecutor {
  // 创建一个基本的 HTTP 客户端
  implicit val backend: SyncBackend = HttpURLConnectionBackend()
  implicit val formats: Formats = DefaultFormats

  val extractUrl = uri"http://10.0.82.200:52109/material"

  override def infer(node: LynxNode, props: Seq[LynxPropertyKey] = Seq.empty): LynxNode = {
    val file: File = node.property(LynxPropertyKey("file")).map {
      case v: LynxString => new File(v.value)
      case _ => throw new RuntimeException("file property is not a string")
    }.getOrElse(throw new RuntimeException("file property not found"))

    val extractRequest = basicRequest
      .post(extractUrl)
      .multipartBody(multipartFile("image", file))

    // 发送请求并获取响应
    val extractResponse = extractRequest.send(backend)
    extractResponse.body match {
      case Right(response) => {
        val j = JsonMethods.parse(response)
        val label = (j \ "label").as[String]
        val n = node.asInstanceOf[VTNode]
        n.update(Map(LynxPropertyKey("material") -> LynxString(label)))
      }
      case Left(error) => throw new RuntimeException(s"Error: $error")
    }
  }
}

object ShapeInfer extends InferLabelExecutor {
  // 创建一个基本的 HTTP 客户端
  implicit val backend: SyncBackend = HttpURLConnectionBackend()
  implicit val formats: Formats = DefaultFormats

  val extractUrl = uri"http://10.0.82.200:52109/shape"
  case class Shape(label: String, score: Double)

  def inferValue(node: LynxNode): Seq[LynxNodeLabel] = {
    val file: File = node.property(LynxPropertyKey("file")).map {
      case v: LynxString => new File(v.value)
      case _ => throw new RuntimeException("file property is not a string")
    }.getOrElse(throw new RuntimeException("file property not found"))

    val extractRequest = basicRequest
      .post(extractUrl)
      .multipartBody(multipartFile("image", file))

    // 发送请求并获取响应
    val extractResponse = extractRequest.send(backend)

    extractResponse.body match {
      case Right(response) => {
        val j = JsonMethods.parse(response)
        val label = (j \ "label").as[String]
        Seq(label).map(LynxNodeLabel.apply)
      }
      case Left(error) => throw new RuntimeException(s"Error: $error")
    }
  }

  override def infer(node: LynxNode): LynxNode = {
    val n = node.asInstanceOf[VTNode]
    n.update(inferValue(n))
  }
}

object PositionInfer extends InferLinkExecutor {
  def pos(box1: List[Int], box2: List[Int]): Seq[String] = {
    // 解构两个边界框的参数
    val List(x1, y1, w1, h1) = box1
    val List(x2, y2, w2, h2) = box2

    // 计算边界框的边界位置
    val (bbox1Left, bbox1Right) = (x1, x1 + w1)
    val (bbox1Top, bbox1Bottom) = (y1, y1 + h1)
    val (bbox2Left, bbox2Right) = (x2, x2 + w2)
    val (bbox2Top, bbox2Bottom) = (y2, y2 + h2)


    val horizontal = if (bbox1Left < bbox2Left  && bbox2Right < bbox1Right) {
      // inside
      if ((bbox2Right+bbox2Left)>(bbox1Left+bbox1Right)) "right" else "left"
    } else {
      // no inside
      if (bbox2Left>bbox1Left) "right" else "left"
    }

    val vertical = if (bbox2Bottom<bbox1Bottom) "behind" else "front"

    Seq(horizontal, vertical)
  }


  override def infer(node: LynxNode, nodes: Seq[LynxNode]): Seq[(LynxNode, LynxRelationship, LynxNode)] = {
//    val pos = List("front", "behind", "left", "right")
    // random select one
    val box = node.property(LynxPropertyKey("box")) match {
      case Some(LynxList(v: List[LynxNumber])) => v.map(_.number.intValue())
      case _ => throw LynxException("Property box not find or not match")
    }
    nodes.flatMap { n =>
      val box2 = n.property(LynxPropertyKey("box")) match {
        case Some(LynxList(v: List[LynxNumber])) => v.map(_.number.intValue())
        case _ => throw LynxException("Property box not find or not match")
      }
      pos(box, box2).map{ relType =>
        (node, TestRelationship(
          TestId.nextId,
          node.id.asInstanceOf[TestId],
          n.id.asInstanceOf[TestId],
          Some(LynxRelationshipType(relType)), Map.empty), n)
      }
    }
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


object GoldContainsInfer extends InferExpandExecutor {

  val data: Map[Int, GoldData.Object] = GoldData.objects

  override def infer(node: LynxNode): Seq[(LynxRelationship, LynxNode)] = {

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
    val id = node.id.toLynxInteger.value
    val obj = GoldData.objects(id.toInt)
    Seq(LynxNodeLabel(obj.shape))
  }
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
