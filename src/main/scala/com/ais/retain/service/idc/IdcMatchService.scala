package com.ais.retain.service.idc

import java.util.concurrent.atomic.AtomicLong

import com.ais.common.LoggerObject
import com.ais.common.container.MultiContainer
import com.ais.retain.container.KeyContainer
import com.ais.retain.service.idc.appender.IdcMatchAppender
import com.ais.retain.service.idc.comparator.IdcMatchComparator
import com.ais.retain.service.idc.converter.{IdcDpiConverter, IdcDpiDetail}
import com.ais.retain.service.pre.converter.RadiusDetail
import com.ais.common.tools.partitioner.IndexPartitioner
import com.ais.retain.creator.ObjectCreator
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/** Created by ShiGZ on 2017/5/10. */
class IdcMatchService(settings: MultiContainer,
                      idcDpiConverter: IdcDpiConverter,
                      idcMatcher: IdcMatcher,
                      idcMatchRePartitioner: IndexPartitioner) extends LoggerObject {

  def doIdcMatch(idcDpiData: RDD[String]): Unit = {
    idcMatcher.idcMatch(idcDpiConverter.convert(idcDpiData).partitionBy(idcMatchRePartitioner))
  }

}

class IdcMatcher(settings: MultiContainer) extends LoggerObject {

  def idcMatch(idcDpiData: RDD[(String, IdcDpiDetail)]): Unit = {
    idcDpiData.mapPartitionsWithIndex((index, iterator) => {
      val result = new ListBuffer[String]
      if (iterator.nonEmpty) {
        val radiusConverter = ObjectCreator.createRadiusConverterByIndex(settings, index)
        val radiusMap = radiusConverter.convert()
        if (!radiusMap.isEmpty) {
          val (appender, producer) = (ObjectCreator.createIdcMatchAppender(settings), ObjectCreator.createKafkaProducer(settings))
          try {
            val atomLong = new AtomicLong
            while (iterator.hasNext) {
              val (privateIp, idcDpiDetail) = iterator.next()
              var currentMatchResultCount = 0
              if (radiusMap.containsKey(privateIp)) {
                val resultOption = getResult(privateIp, idcDpiDetail, radiusMap.get(privateIp).toIterator, appender)
                if (resultOption.isDefined) {
                  producer.send(new ProducerRecord[String, String](settings.getTextByKey(KeyContainer.KEY_KAFKA_IDC_RESULT_TOPIC_NAME), atomLong.incrementAndGet().toString, resultOption.get))
                  currentMatchResultCount = currentMatchResultCount + 1
                  if (currentMatchResultCount >= settings.getIntValueByKey(KeyContainer.KEY_KAFKA_BATCH_COUNT)) {
                    producer.flush()
                    currentMatchResultCount = 0
                  }
                }
              }
            }
          } catch {
            case ex: Exception => errorLog(ex, "idc 匹配过程中出现了异常！异常信息为：", ex.getCause)
          } finally {
            producer.flush()
            producer.close()
          }
        }
      }
      result.toIterator
    }).count()
  }

  private def getResult(privateIp: String,
                        idcDpiDetail: IdcDpiDetail,
                        radiusIt: Iterator[RadiusDetail],
                        appender: IdcMatchAppender): Option[String] = {
    while (radiusIt.hasNext) {
      val radiusDetail = radiusIt.next()
      if (IdcMatchComparator.isMatch(radiusDetail, idcDpiDetail)) {
        return Some(appender.append(privateIp,radiusDetail, idcDpiDetail))
      }
    }
    None
  }

}