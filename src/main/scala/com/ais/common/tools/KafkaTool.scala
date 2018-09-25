package com.ais.common.tools

import java.util.Properties

import com.ais.common.LoggerObject
import org.apache.kafka.clients.producer.{KafkaProducer, Producer}

/** Created by ShiGZ on 2016/8/26. */
object KafkaTool extends LoggerObject {

  /**
    *
    * @param brokerList brokerList
    * @return 生产者参数
    */
  def getProducerConf(brokerList: String): Properties = {
    val result = new Properties()
    result.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    result.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    result.put("bootstrap.servers", brokerList)
    result.put("acks", "1")
    result
  }

  /**
    *
    * @param brokerList       brokerList
    * @param batchCountOption 批量数
    * @return 批量生产者参数
    */
  def getProducerConf(brokerList: String, batchCountOption: Option[String]): Properties = {
    val result = getProducerConf(brokerList)
    if (batchCountOption.isDefined) {
      result.put("batch.num.messages", batchCountOption.get)
    }
    result
  }

  def getProducerConf(brokerList: String, batchCount: String): Properties = {
    val result = getProducerConf(brokerList)
    result.put("compression.codec", "snappy")
    result.put("batch.size", batchCount)
    result.put("linger.ms", "1")
    result.put("buffer.memory", "33554432")
    result
  }

  def getProducer(brokerList: String, batchCount: String): Producer[String, String] = {
    new KafkaProducer[String, String](getProducerConf(brokerList, batchCount))
  }

}