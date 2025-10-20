package org.grapheco.evaluation

import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import vision.{Vision, VisionServiceGrpc}

import java.io.{File, FileInputStream}
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.util.{Failure, Success, Try}

// 创建 gRPC 客户端
case class VisionClient(host: String, port: Int) {
  // 创建 channel，用于连接服务器
  private val channel: ManagedChannel = ManagedChannelBuilder
    .forAddress(host, port)
    .usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build()

  // 创建 blocking stub，用于同步调用 gRPC 服务
  private val blockingStub: VisionServiceGrpc.VisionServiceBlockingStub =
    VisionServiceGrpc.newBlockingStub(channel)

  // 图像分割方法
  def segment(imageFile: File): Try[List[Vision.SegmentMask]] = {
    Try {
      val imageBytes = readImageFile(imageFile)
      val request = Vision.SegmentRequest.newBuilder()
        .setImage(ByteString.copyFrom(imageBytes))
        .setFilename(imageFile.getName)
        .build()
      val response = blockingStub.segment(request)
      response.getMasksList.asScala.toList
    } match {
      case Success(result) => Success(result)
      case Failure(exception) => Failure(exception)
    }
  }

  // 图像分类方法
  def classify(imageFile: File, categoryType: String): Try[Vision.ClassifyResponse] = {
    Try {
      val imageBytes = readImageFile(imageFile)
      val request = Vision.ClassifyRequest.newBuilder()
        .setImage(ByteString.copyFrom(imageBytes))
        .setFilename(imageFile.getName)
        .setCategoryType(categoryType)
        .build()
      blockingStub.classify(request)
    } match {
      case Success(result) => Success(result)
      case Failure(exception) => println(exception); Failure(exception)
    }
  }

  def test(name: String): Try[Vision.TestResponse] = {
    Try {
      val request = Vision.TestRequest.newBuilder()
        .setName(name)
        .build()
      blockingStub.test(request)
    } match {
      case Success(result) => Success(result)
      case Failure(exception) => Failure(exception)
    }
  }

  // 读取图像文件为字节数组
  private def readImageFile(file: File): Array[Byte] = {
    val fis = new FileInputStream(file)
    try {
      val bytes = new Array[Byte](file.length().toInt)
      fis.read(bytes)
      bytes
    } finally {
      fis.close()
    }
  }

  // 关闭 channel
  def shutdown(): Unit = {
    channel.shutdown()
  }
}

