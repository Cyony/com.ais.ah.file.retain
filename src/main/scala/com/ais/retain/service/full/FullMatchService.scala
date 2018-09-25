package com.ais.retain.service.full

import java.io.{BufferedReader, InputStreamReader}
import java.util
import java.util.concurrent.atomic.AtomicLong

import com.ais.common.LoggerObject
import com.ais.common.container.MultiContainer
import com.ais.common.tools.partitioner.IndexPartitioner
import com.ais.common.tools.{HadoopTool, StringTool}
import com.ais.retain.container.KeyContainer
import com.ais.retain.creator.ObjectCreator
import com.ais.retain.service.full.appender.FullMatchAppender
import com.ais.retain.service.full.comparator.FullMatchComparator
import com.ais.retain.service.full.converter.{DpiConverter, DpiDetail, PreMatchDetail}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.util.ReflectionUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/** Created by ShiGZ on 2017/5/10. */
class FullMatchService(dpiConverter: DpiConverter,
                       fullMatcher: FullMatcher,
                       fullMatchRePartitioner: IndexPartitioner) extends LoggerObject {

  def doFullMatch(dpiData: RDD[String], earliestTime: Long,latestTime:Long): Unit = {
    fullMatcher.fullMatch(dpiConverter.convert(dpiData).partitionBy(fullMatchRePartitioner), earliestTime,latestTime)
  }

}

class FullMatcher(settings: MultiContainer) extends LoggerObject {

  def fullMatch(dpiData: RDD[(String, DpiDetail)], earliestTime: Long,latestTime:Long): Unit = {
    dpiData.mapPartitionsWithIndex((index, iterator) => {
      val result = new ListBuffer[String]
      if (iterator.nonEmpty) {
        val dpiDetailMap = new util.HashMap[String, ListBuffer[DpiDetail]]()
        while (iterator.hasNext) {
          val (keyMsg, dpiDetail) = iterator.next()
          if (dpiDetailMap.containsKey(keyMsg)) {
            val dpiDetailLB = dpiDetailMap.get(keyMsg)
            dpiDetailLB.append(dpiDetail)
            dpiDetailMap.put(keyMsg, dpiDetailLB)
          } else {
            val dpiDetailLB = new ListBuffer[DpiDetail]()
            dpiDetailLB.append(dpiDetail)
            dpiDetailMap.put(keyMsg, dpiDetailLB)
          }
        }
        val atomLong = new AtomicLong
        var currentMatchResultCount = 0
        val (appender, producer, parser) = (ObjectCreator.createFullMatchAppender(settings), ObjectCreator.createKafkaProducer(settings), ObjectCreator.createPreMatchParser(settings))
        val rootDirPath = StringTool.joinValues(settings.getTextByKey(KeyContainer.KEY_HADOOP_PRE_MATCH_DIR_PATH), "/", index)
        val fileNameList = HadoopTool.getFileNameABFromPath(rootDirPath,earliestTime,latestTime)
        val fs = FileSystem.newInstance(HadoopTool.initHadoopConfig())
        try {
          for (fileName <- fileNameList) {
            val fsIn = fs.open(new Path(StringTool.joinValues(rootDirPath, "/", fileName)))
            val codec =ReflectionUtils.newInstance(Class.forName(settings.getTextByKey(KeyContainer.KEY_HADOOP_PRE_MATCH_COMPRESSION_CODEC)),HadoopTool.initHadoopConfig()).asInstanceOf[CompressionCodec]
            val compressedFsIn = codec.createInputStream(fsIn)
            val inputReader = new InputStreamReader(compressedFsIn)
            val bufferReader = new BufferedReader(inputReader)
            try {
              var preMatchLog = ""
              while (preMatchLog != null) {
                if (preMatchLog.nonEmpty) {
                  val preMatchDetailOption = parser.parse(preMatchLog)
                  if (preMatchDetailOption.isDefined) {
                    val (keyMsg, preMatchDetail) = preMatchDetailOption.get
                    if (dpiDetailMap.containsKey(keyMsg)) {
                      val resultOption = getMatchResult(keyMsg, preMatchDetail, dpiDetailMap.get(keyMsg).toIterator, appender)
                      if (resultOption.isDefined) {
                        producer.send(new ProducerRecord[String, String](settings.getTextByKey(KeyContainer.KEY_KAFKA_RESULT_TOPIC_NAME), atomLong.incrementAndGet().toString, resultOption.get))
                        currentMatchResultCount = currentMatchResultCount + 1
                        if (currentMatchResultCount >= settings.getIntValueByKey(KeyContainer.KEY_KAFKA_BATCH_COUNT)) {
                          producer.flush()
                          currentMatchResultCount = 0
                        }
                      }
                    }
                  }
                }
                preMatchLog = bufferReader.readLine()
              }
            } catch {
              case ex: Exception => errorLog(ex, "匹配过程中出现了异常！异常信息为：", ex.getCause)
            } finally {
              inputReader.close()
              bufferReader.close()
              compressedFsIn.close()
              fsIn.close()
            }
          }
        } catch {
          case ex: Exception => errorLog(ex, "全匹配过程中出现了异常！异常信息为：", ex.getCause)
        } finally {
          fs.close()
          producer.flush()
          producer.close()
        }
      }
      result.toIterator
    }).count()
  }


  private def getMatchResult(keyMsg: String,
                             preMatchDetail: PreMatchDetail,
                             dpiDetailIt: Iterator[DpiDetail],
                             appender: FullMatchAppender): Option[String] = {
    while (dpiDetailIt.hasNext) {
      val dpiDetail = dpiDetailIt.next()
      if (FullMatchComparator.isMatch(preMatchDetail, dpiDetail, settings.getIntValueByKey(KeyContainer.KEY_HADOOP_DPI_LAG_MILLIS))) {
        return Some(appender.append(keyMsg, preMatchDetail, dpiDetail))
      }
    }
    None
  }

}