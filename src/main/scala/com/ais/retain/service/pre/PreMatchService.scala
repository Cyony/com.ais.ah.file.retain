package com.ais.retain.service.pre

import com.ais.common.LoggerObject
import com.ais.common.container.MultiContainer
import com.ais.common.tools.partitioner.IndexPartitioner
import com.ais.common.tools.{HadoopTool, StringTool}
import com.ais.retain.container.KeyContainer
import com.ais.retain.creator.ObjectCreator
import com.ais.retain.service.pre.appender.PreMatchAppender
import com.ais.retain.service.pre.comparator.PreMatchComparator
import com.ais.retain.service.pre.converter.{NatConverter, NatDetail, RadiusDetail}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.spark.io.SnappyCompressionCodec
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/** Created by ShiGZ on 2017/5/10. */
class PreMatchService(settings: MultiContainer,
                      natConverter: NatConverter,
                      preMatcher: PreMatcher,
                      preMatchRePartitioner: IndexPartitioner,
                      fullMatchRePartitioner: IndexPartitioner) extends LoggerObject {

  def doPreMatch(natData: RDD[String]): Unit = {
    preMatcher.preMatch(natConverter.convert(natData).partitionBy(preMatchRePartitioner)).partitionBy(fullMatchRePartitioner).mapPartitionsWithIndex((index, iterator) => {
      val result = new ListBuffer[String]
      if (iterator.nonEmpty) {
        val indexDirPathName = StringTool.joinValues(settings.getTextByKey(KeyContainer.KEY_HADOOP_PRE_MATCH_DIR_PATH), "/", index, "/")
        val tempFileName = StringTool.joinValues(System.currentTimeMillis(), ".TMP")
        val fs = FileSystem.newInstance(HadoopTool.initHadoopConfig())
        val tempPath = new Path(indexDirPathName + tempFileName)
        val preMatchOutputStream = fs.create(tempPath)
        val codec =ReflectionUtils.newInstance(Class.forName(settings.getTextByKey(KeyContainer.KEY_HADOOP_PRE_MATCH_COMPRESSION_CODEC)),HadoopTool.initHadoopConfig()).asInstanceOf[CompressionCodec]
        val compressedPreMatchOutputStream = codec.createOutputStream(preMatchOutputStream)
        var currentCount = 0
        try {
          while (iterator.hasNext) {
            val (keyMsg, msg) = iterator.next()
            compressedPreMatchOutputStream.write(StringTool.joinValues(keyMsg, settings.getTextByKey(KeyContainer.KEY_PRE_MATCH_FIELD_SEPARATOR), msg, "\n").getBytes())
            currentCount = currentCount + 1
            if (currentCount >= 100000) {
              preMatchOutputStream.hflush()
              currentCount = 0
            }
          }
        } catch {
          case ex: Exception => errorLog(ex, "将预匹配结果写入到 hadoop 的过程中出现了异常！异常信息为：", ex.getStackTrace)
        } finally {
          compressedPreMatchOutputStream.close()
          preMatchOutputStream.close()
          fs.close()
          HadoopTool.renameFile(indexDirPathName, tempFileName, indexDirPathName + System.currentTimeMillis())
        }
      }
      result.toIterator
    }).count()
  }

}

class PreMatcher(settings: MultiContainer) extends LoggerObject {

  def preMatch(natData: RDD[(String, NatDetail)]): RDD[(String, String)] = {
    natData.mapPartitionsWithIndex((index, iterator) => {
      val result = new ListBuffer[(String, String)]
      if (iterator.nonEmpty) {
        val radiusConverter = ObjectCreator.createRadiusConverterByIndex(settings, index)
        val radiusMap = radiusConverter.convert()
        if (!radiusMap.isEmpty) {
          val appender = ObjectCreator.createPreMatchAppender(settings)
          try {
            while (iterator.hasNext) {
              val (privateIp, natDetail) = iterator.next()
              if (radiusMap.containsKey(privateIp)) {
                val resultOption = getPreMatchResult(privateIp, natDetail, radiusMap.get(privateIp).toIterator, appender)
                if (resultOption.isDefined) {
                  result.append(resultOption.get)
                }
              }
            }
          } catch {
            case ex: Exception => errorLog(ex, "预匹配过程中出现了异常！异常信息为：", ex.getCause)
          }
        }
      }
      result.toIterator
    })
  }

  private def getPreMatchResult(privateIp: String,
                                natDetail: NatDetail,
                                radiusIt: Iterator[RadiusDetail],
                                appender: PreMatchAppender): Option[(String, String)] = {
    while (radiusIt.hasNext) {
      val radiusDetail = radiusIt.next()
      if (PreMatchComparator.isMatch(radiusDetail, natDetail)) {
        return Some(appender.appendPublicKeyMsg(privateIp, natDetail), appender.appendMsg(radiusDetail, natDetail))
      }
    }
    None
  }

}