package org.grapheco.evaluation

import org.grapheco.lynx.runner.infer.{InferExpandExecutor, InferLabelExecutor, InferPropertyExecutor}
import org.grapheco.lynx.types.property.LynxString
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
          val e = TestRelationship(
            TestId.none,
            TestId.none,
            TestId.none,
            Some(LynxRelationshipType("contains")),
            Map.empty
          )
          val str = s"cache/${System.currentTimeMillis()}.png"
          val n = TestNode(
            TestId.none,
            Seq(),
            Map(LynxPropertyKey("file") -> LynxString(str))
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