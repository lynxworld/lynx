package org.grapheco.virtual

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.runner.CypherRunner
import org.grapheco.lynx.runner.infer.{Condition, InferAdviser, InferEngine, InferExpandExecutor, RemoteInferAdviser}
import org.grapheco.lynx.types.property.LynxString
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.native.JsonMethods
import sttp.client4.httpurlconnection.HttpURLConnectionBackend
import sttp.client4.{SyncBackend, UriContext, basicRequest, multipartFile}

import java.awt.Rectangle
import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO

class VirtualTestBase extends TestBase {
  override val runner: CypherRunner = new CypherRunner(graphModel = model) {
    val adviser: InferAdviser =
      RemoteInferAdviser().addInfer(
        Condition(relType = Seq("contains")), ContainsInfer)
    override protected lazy val inferEngine: InferEngine = InferEngine.remote.withAdviser(adviser)
  }

  object ContainsInfer extends InferExpandExecutor {
    // 创建一个基本的 HTTP 客户端
    implicit val backend: SyncBackend = HttpURLConnectionBackend()
    implicit val formats: Formats = DefaultFormats

    val extractUrl = uri"http://10.0.82.214:8000/extract/"

    case class Box(score: Double, label: String, box: List[Double])

    override def infer(node: LynxNode): Seq[(LynxRelationship, LynxNode)] = {
      val file: File = node.property(LynxPropertyKey("file")).map {
        case v: LynxString => new File(v.value)
        case _ => throw new RuntimeException("file property is not a string")
      }.getOrElse(throw new RuntimeException("file property not found"))

      val extractRequest = basicRequest
        .post(extractUrl)
        .multipartBody(multipartFile("file", file))

      // 发送请求并获取响应
      val extractResponse = extractRequest.send(backend)
      extractResponse.body match {
        case Right(response) => {
          val j = JsonMethods.parse(response)
          (j \ "boxes").extract[List[Box]].map {
            case Box(score, label, box) => {
              val path = s"${file.getParentFile().getAbsolutePath()}/${System.currentTimeMillis()}.jpg"
              cropImage(file.getAbsolutePath,
                new Rectangle(box(0).toInt, box(1).toInt, box(2).toInt - box(0).toInt, box(3).toInt - box(1).toInt),
                path)
              (TestRelationship(TestId(0), TestId(0), TestId(0), Option(LynxRelationshipType("contains")), Map.empty),
                TestNode(TestId(0), Seq(LynxNodeLabel(label)), Map(LynxPropertyKey("file")->LynxString(path))))
            }
          }
        }
        case Left(error) => throw new RuntimeException(s"Error: $error")
      }
    }

    def cropImage(imagePath: String, box: Rectangle, savePath: String): Unit = {
      // 加载原图
      val originalImage: BufferedImage = ImageIO.read(new File(imagePath))

      // 切割图像
      val croppedImage = originalImage.getSubimage(box.x, box.y, box.width, box.height)

      // 保存切割后的小图
      val outputFile = new File(savePath)
      ImageIO.write(croppedImage, "jpg", outputFile)

      println(s"Cropped image saved to ${outputFile.getAbsolutePath}")
    }
  }
}
