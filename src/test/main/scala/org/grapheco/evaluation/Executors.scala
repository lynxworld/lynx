package org.grapheco.evaluation

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

  case class Mask(area: Int, bbox: List[Int], predicted_iou: Double, segmentation: List[List[Int]])

  override def infer(node: LynxNode): Seq[(LynxRelationship, LynxNode)] = {
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
        j.extract[List[Mask]].map { mask =>
          val outId = TestId.nextId
          val e = TestRelationship(
            TestId.nextId,
            node.id.asInstanceOf[TestId],
            outId,
            Some(LynxRelationshipType("contains")),
            Map.empty
          )
          val str = s"cache/${System.currentTimeMillis()}.png"
          val n = TestNode(
            outId,
            Seq(),
            Map(
              LynxPropertyKey("file") -> LynxString(str),
              LynxPropertyKey("size") -> LynxString(if(mask.area>200)"large" else "small"),
              LynxPropertyKey("box") -> LynxList(mask.bbox.map(LynxValue.apply))
            )
          )
          cropImage(file.getAbsolutePath, new Rectangle(mask.bbox(0), mask.bbox(1), mask.bbox(2), mask.bbox(3)), mask.segmentation, str)
          (e, n)
        }
      }
      case Left(error) => throw new RuntimeException(s"Error: $error")
    }
  }

  def cropImage(imagePath: String, box: Rectangle, mask: List[List[Int]], savePath: String): Unit = {
    // 加载原图
    val originalImage: BufferedImage = ImageIO.read(new File(imagePath))

    // 切割图像
    val croppedImage = originalImage.getSubimage(box.x, box.y, box.width, box.height)

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
//    val maskedImage = croppedImage

    // 保存切割后的小图
    val outputFile = new File(savePath)
    ImageIO.write(maskedImage, "png", outputFile)  // 使用 PNG 格式以支持透明度

//    println(s"Cropped image saved to ${outputFile.getAbsolutePath}")
  }
}

case class LabelScore(label: String, score: Double)

object ColorInfer extends InferPropertyExecutor {
  // 创建一个基本的 HTTP 客户端
  implicit val backend: SyncBackend = HttpURLConnectionBackend()
  implicit val formats: Formats = DefaultFormats

  val extractUrl = uri"http://10.0.82.200:52109/color"

  override def infer(node: LynxNode): LynxNode = {
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
        val color = j.extract[List[LabelScore]].maxBy(_.score).label
        val n = node.asInstanceOf[TestNode]
        n.copy(props = n.props + (LynxPropertyKey("color") -> LynxString(color)))
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

  override def infer(node: LynxNode): LynxNode = {
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
        val color = (j \ "label").as[String]
        val m = if (color=="metal") "metal" else "rubber"
        val n = node.asInstanceOf[TestNode]
        n.copy(props = n.props + (LynxPropertyKey("material") -> LynxString(m)))
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
  override def infer(node: LynxNode): LynxNode = {
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
        val label = j.extract[List[Shape]].maxBy(_.score).label
        val n = node.asInstanceOf[TestNode]
        n.copy(labels = n.labels.+:(LynxNodeLabel(label)))
      }
      case Left(error) => throw new RuntimeException(s"Error: $error")
    }
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