package org.grapheco.evaluation

import com.github.tototoshi.csv.CSVReader
import org.grapheco.lynx.LynxException
import org.grapheco.lynx.infer.{InferExpandExecutor, InferLabelExecutor, InferLinkExecutor, InferPropertyExecutor}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxInteger, LynxNumber, LynxString}
import org.grapheco.lynx.types.structural._
import vision.Vision

import java.awt.image.BufferedImage
import java.awt.{Color, Rectangle}
import java.io.File
import javax.imageio.ImageIO
import scala.util.Random
import scala.util.Random.javaRandomToRandom

object Clients {
  val host = "10.0.82.200"
  val port = 50051
  val client = new VisionClient(host, port)
}

object ContainsInfer extends InferExpandExecutor {

  val size_threshold = 20.06

  def size(mask: Vision.SegmentMask): Boolean = {
    val centerY = mask.getBbox.getY + mask.getBbox.getHeight / 2
    mask.getArea / centerY > size_threshold
  }

  implicit def bbox2list: Vision.BoundingBox => List[Int] = b => List(b.getX, b.getY, b.getWidth, b.getHeight).map(_.toInt)

  override def infer(node: LynxNode): Seq[(LynxRelationship, LynxNode)] = {
    val time0 = System.currentTimeMillis()
    val file: File = node.property(LynxPropertyKey("file")).map {
      case v: LynxString => new File(v.value)
      case _ => throw new RuntimeException("file property is not a string")
    }.getOrElse(throw new RuntimeException("file property not found"))

    val time1 = System.currentTimeMillis()
    val maybeResult = Clients.client.segment(file).toOption
    val time2 = System.currentTimeMillis()
    val out = maybeResult.map { results =>
      results.map { mask =>
        val outId = TestId.nextId
        val e = TestRelationship(
          TestId.nextId,
          node.id.asInstanceOf[TestId],
          outId,
          Some(LynxRelationshipType("contains")),
          Map.empty
        )
        val str = s"cache/${System.currentTimeMillis()}.png"
        val n = VTNode(
          outId,
          Seq(),
          Map(
            LynxPropertyKey("file") -> LynxString(str),
            LynxPropertyKey("area") -> LynxInteger(mask.getArea),
            LynxPropertyKey("size") -> LynxString(if (size(mask)) "large" else "small"),
            LynxPropertyKey("box") -> LynxList(mask.getBbox.map(LynxValue.apply))
          ),
          getLabels = ShapeInfer.inferValue
        )
        cropImage(file.getAbsolutePath, mask.getBbox, List.empty, str)

        (e, n)
      }
    }
//    val time3 = System.currentTimeMillis()
//    println(s"${time1-time0}, ${time2-time1}, ${time3-time2}")
    out.getOrElse(Seq.empty)
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

case class PropertyInfer(category: String) extends InferPropertyExecutor {

  override def infer(node: LynxNode, props: Seq[LynxPropertyKey] = Seq.empty): LynxNode = {
    val file: File = node.property(LynxPropertyKey("file")).map {
      case v: LynxString => new File(v.value)
      case _ => throw new RuntimeException("file property is not a string")
    }.getOrElse(throw new RuntimeException("file property not found"))

    Clients.client.classify(file, category).map{ result =>
      val label = result.getLabel
      val n = node.asInstanceOf[VTNode]
      n.update(Map(LynxPropertyKey(category) -> LynxString(label)))
    }.getOrElse(node)
  }
}

object ColorInfer extends PropertyInfer("color")

object MaterialInfer extends PropertyInfer("material")

object ShapeInfer extends InferLabelExecutor {

  def inferValue(node: LynxNode): Seq[LynxNodeLabel] = {
    val file: File = node.property(LynxPropertyKey("file")).map {
      case v: LynxString => new File(v.value)
      case _ => throw new RuntimeException("file property is not a string")
    }.getOrElse(throw new RuntimeException("file property not found"))

    Clients.client.classify(file, "shape").map{ result =>
      val label = result.getLabel
      Seq(LynxNodeLabel(label))
    }.getOrElse(Seq.empty)
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
      if (node.id == n.id) {
        Seq()
      }else {
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
}






