package com.ais.retain.creator

import com.ais.common.container.MultiContainer
import com.ais.common.tools.fetcher.{LongFetcher, MultiFetcher, TextFetcher, TimeFetcher}
import com.ais.common.tools.partitioner.IndexPartitioner
import com.ais.common.tools.spiltter.Splitter
import com.ais.common.tools.{FileTool, HBaseTool, KafkaTool}
import com.ais.retain.container.KeyContainer
import com.ais.retain.service.full.appender.FullMatchAppender
import com.ais.retain.service.full.converter._
import com.ais.retain.service.full.{FullMatchService, FullMatcher}
import com.ais.retain.service.idc.appender.IdcMatchAppender
import com.ais.retain.service.idc.converter.{IdcDpiConverter, IdcDpiParser}
import com.ais.retain.service.idc.{IdcMatchService, IdcMatcher}
import com.ais.retain.service.pre.appender.PreMatchAppender
import com.ais.retain.service.pre.converter._
import com.ais.retain.service.pre.{PreMatchService, PreMatcher}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.producer.Producer

/** Created by ShiGZ on 2017/5/9. */
object ObjectCreator {

  val HADOOP_COMPRESSION_CODEC_MAP = Map("deflate" -> "org.apache.hadoop.io.compress.DefaultCodec","gzip" -> "org.apache.hadoop.io.compress.GzipCodec","bzip2" -> "org.apache.hadoop.io.compress.BZip2Codec","snappy" -> "org.apache.hadoop.io.compress.SnappyCodec","lzo" -> "com.hadoop.compression.lzo.LzopCodec")

  def createRadiusParser(settings: MultiContainer): RadiusParser = {
    new RadiusParser(Splitter.getInstance(settings.getTextByKey(KeyContainer.KEY_RADIUS_FIELD_SEPARATOR)),
      Splitter.getInstance(settings.getTextByKey(KeyContainer.KEY_RADIUS_ROW_KEY_SEPARATOR)),
      TimeFetcher.getInstance(settings.getIntValueByKey(KeyContainer.KEY_RADIUS_END_TIME_INDEX), settings.getIntOptionByKey(KeyContainer.KEY_RADIUS_TIME_VALID_DIGITS), settings.getTextOptionByKey(KeyContainer.KEY_RADIUS_TIME_FORMAT), settings.getIntOptionByKey(KeyContainer.KEY_RADIUS_TIME_RETAIN_DIGITS)),
      MultiFetcher.getInstance(settings.getIntArrByKey(KeyContainer.KEY_RADIUS_NORMAL_MSG_INDEXES), settings.getTextByKey(KeyContainer.KEY_APP_FINAL_SEPARATOR))
    )
  }

  def createRadiusConverterByIndex(settings: MultiContainer, partitionIndex: Int): RadiusConverter = {
    new RadiusConverter(createRadiusParser(settings),
      HBaseTool.hBaseConfig(settings.getTextByKey(KeyContainer.KEY_H_BASE_HOST_MASTER), settings.getTextByKey(KeyContainer.KEY_H_BASE_ZK_QUORUM), settings.getTextByKey(KeyContainer.KEY_H_BASE_ZK_PORT)),
      TableName.valueOf(settings.getTextByKey(KeyContainer.KEY_H_BASE_RADIUS_TABLE_NAME_PREFIX) + partitionIndex),
      Bytes.toBytes(settings.getTextByKey(KeyContainer.KEY_H_BASE_FAMILY_NAME)),
      Bytes.toBytes(settings.getTextByKey(KeyContainer.KEY_H_BASE_DETAIL_COLUMN_NAME)),
      HBaseTool.initialScan(settings.getIntValueByKey(KeyContainer.KEY_H_BASE_SCAN_CACHING_COUNT), settings.getIntValueByKey(KeyContainer.KEY_H_BASE_SCAN_BATCH_COUNT))
    )
  }

  def createNatParser(settings: MultiContainer): NatParser = {
    new NatParser(
      Splitter.getInstance(settings.getTextByKey(KeyContainer.KEY_NAT_FIELD_SEPARATOR)),
      TextFetcher.getInstance(settings.getIntValueByKey(KeyContainer.KEY_NAT_PRIVATE_IP_INDEX)),
      TimeFetcher.getInstance(settings.getIntValueByKey(KeyContainer.KEY_NAT_START_TIME_INDEX), settings.getIntOptionByKey(KeyContainer.KEY_NAT_TIME_VALID_DIGITS), settings.getTextOptionByKey(KeyContainer.KEY_NAT_TIME_FORMAT), settings.getIntOptionByKey(KeyContainer.KEY_NAT_TIME_RETAIN_DIGITS)),
      TimeFetcher.getInstance(settings.getIntValueByKey(KeyContainer.KEY_NAT_END_TIME_INDEX), settings.getIntOptionByKey(KeyContainer.KEY_NAT_TIME_VALID_DIGITS), settings.getTextOptionByKey(KeyContainer.KEY_NAT_TIME_FORMAT), settings.getIntOptionByKey(KeyContainer.KEY_NAT_TIME_RETAIN_DIGITS)),
      MultiFetcher.getInstance(settings.getIntArrByKey(KeyContainer.KEY_NAT_KEY_MSG_INDEXES), settings.getTextByKey(KeyContainer.KEY_APP_FINAL_SEPARATOR)),
      MultiFetcher.getInstance(settings.getIntArrByKey(KeyContainer.KEY_NAT_NORMAL_MSG_INDEXES), settings.getTextByKey(KeyContainer.KEY_APP_FINAL_SEPARATOR))
    )
  }

  def createNatConverter(settings: MultiContainer): NatConverter = {
    new NatConverter(settings)
  }

  def createPreMatchAppender(settings: MultiContainer): PreMatchAppender = {
    new PreMatchAppender(settings.getTextByKey(KeyContainer.KEY_APP_FINAL_SEPARATOR), "")
  }

  def createPreMatcher(settings: MultiContainer): PreMatcher = {
    new PreMatcher(settings)
  }

  def createPreMatchService(settings: MultiContainer): PreMatchService = {
    new PreMatchService(settings,
      createNatConverter(settings),
      createPreMatcher(settings),
      new IndexPartitioner(settings.getIntValueByKey(KeyContainer.KEY_H_BASE_RADIUS_TABLE_COUNT)),
      new IndexPartitioner(settings.getIntValueByKey(KeyContainer.KEY_HADOOP_PRE_MATCH_DIR_COUNT)))
  }

  def createDpiParser(settings: MultiContainer): DpiParser = {
    new DpiParser(
      Splitter.getInstance(settings.getTextByKey(KeyContainer.KEY_DPI_FIELD_SEPARATOR)),
      TimeFetcher.getInstance(settings.getIntValueByKey(KeyContainer.KEY_DPI_START_TIME_INDEX),
        settings.getIntOptionByKey(KeyContainer.KEY_DPI_TIME_VALID_DIGITS),
        settings.getTextOptionByKey(KeyContainer.KEY_DPI_TIME_FORMAT),
        settings.getIntOptionByKey(KeyContainer.KEY_DPI_TIME_RETAIN_DIGITS)),
      TimeFetcher.getInstance(settings.getIntValueByKey(KeyContainer.KEY_DPI_END_TIME_INDEX),
        settings.getIntOptionByKey(KeyContainer.KEY_DPI_TIME_VALID_DIGITS),
        settings.getTextOptionByKey(KeyContainer.KEY_DPI_TIME_FORMAT),
        settings.getIntOptionByKey(KeyContainer.KEY_DPI_TIME_RETAIN_DIGITS)),
      MultiFetcher.getInstance(settings.getIntArrByKey(KeyContainer.KEY_DPI_KEY_MSG_INDEXES), settings
        .getTextByKey(KeyContainer.KEY_APP_FINAL_SEPARATOR)),
      MultiFetcher.getInstance(settings.getIntArrByKey(KeyContainer.KEY_DPI_NORMAL_MSG_INDEXES), settings
        .getTextByKey(KeyContainer.KEY_APP_FINAL_SEPARATOR))
    )
  }

  def createDpiConverter(settings: MultiContainer): DpiConverter = {
    new DpiConverter(settings)
  }

  def createPreMatchParser(settings: MultiContainer): PreMatchParser = {
    new PreMatchParser(
      Splitter.getInstance(settings.getTextByKey(KeyContainer.KEY_PRE_MATCH_FIELD_SEPARATOR)),
      MultiFetcher.getInstance(settings.getIntArrByKey(KeyContainer.KEY_PRE_MATCH_KEY_MSG_INDEXES), settings.getTextByKey(KeyContainer.KEY_APP_FINAL_SEPARATOR)),
      LongFetcher.getInstance(settings.getIntValueByKey(KeyContainer.KEY_PRE_MATCH_START_TIME_INDEX)),
      TextFetcher.getInstance(settings.getIntValueByKey(KeyContainer.KEY_PRE_MATCH_END_TIME_INDEX)),
      MultiFetcher.getInstance(settings.getIntArrByKey(KeyContainer.KEY_PRE_MATCH_NORMAL_MSG_INDEXES), settings.getTextByKey(KeyContainer.KEY_APP_FINAL_SEPARATOR))
    )
  }

  def createFullMatchAppender(settings: MultiContainer): FullMatchAppender = {
    new FullMatchAppender(settings.getTextByKey(KeyContainer.KEY_APP_FINAL_SEPARATOR),
      settings.getTextByKey(KeyContainer.KEY_APP_FULL_MATCH_RESULT_LOG_TYPE), "")
  }

  def createFullMatcher(settings: MultiContainer): FullMatcher = {
    new FullMatcher(settings)
  }

  def createFullMatchService(settings: MultiContainer): FullMatchService = {
    new FullMatchService(
      createDpiConverter(settings),
      createFullMatcher(settings),
      new IndexPartitioner(settings.getIntValueByKey(KeyContainer.KEY_HADOOP_PRE_MATCH_DIR_COUNT)))
  }

  def createIdcDpiParser(settings: MultiContainer): IdcDpiParser = {
    new IdcDpiParser(
      Splitter.getInstance(settings.getTextByKey(KeyContainer.KEY_IDC_DPI_FIELD_SEPARATOR)),
      TextFetcher.getInstance(settings.getIntValueByKey(KeyContainer.KEY_IDC_DPI_PRIVATE_IP_INDEX)),
      TimeFetcher.getInstance(settings.getIntValueByKey(KeyContainer.KEY_IDC_DPI_START_TIME_INDEX),
        settings.getIntOptionByKey(KeyContainer.KEY_IDC_DPI_TIME_VALID_DIGITS),
        settings.getTextOptionByKey(KeyContainer.KEY_IDC_DPI_TIME_FORMAT),
        settings.getIntOptionByKey(KeyContainer.KEY_IDC_DPI_TIME_RETAIN_DIGITS)),
      MultiFetcher.getInstance(settings.getIntArrByKey(KeyContainer.KEY_IDC_DPI_NORMAL_MSG_INDEXES), settings.getTextByKey(KeyContainer.KEY_APP_FINAL_SEPARATOR))
    )
  }

  def createIdcDpiConverter(settings: MultiContainer): IdcDpiConverter = {
    new IdcDpiConverter(settings)
  }

  def createIdcMatchAppender(settings: MultiContainer): IdcMatchAppender = {
    new IdcMatchAppender(settings.getTextByKey(KeyContainer.KEY_APP_FINAL_SEPARATOR),
      settings.getTextByKey(KeyContainer.KEY_APP_IDC_MATCH_RESULT_LOG_TYPE), "")
  }

  def createIdcMatcher(settings: MultiContainer): IdcMatcher = {
    new IdcMatcher(settings)
  }

  def createIdcMatchService(settings: MultiContainer): IdcMatchService = {
    new IdcMatchService(settings,
      createIdcDpiConverter(settings),
      createIdcMatcher(settings),
      new IndexPartitioner(settings.getIntValueByKey(KeyContainer.KEY_H_BASE_RADIUS_TABLE_COUNT))
    )
  }

  def createKafkaProducer(settings: MultiContainer): Producer[String, String] = {
    KafkaTool.getProducer(settings.getTextByKey(KeyContainer.KEY_KAFKA_BROKER_LIST), settings.getTextByKey(KeyContainer.KEY_KAFKA_BATCH_COUNT))
  }

  def initSettings(settingPath: String): MultiContainer = {
    val result = new MultiContainer
    val props = FileTool.propertiesFromPath(settingPath)
    //初始化非必填String型配置项
    result.initTextValue(props, KeyContainer.KEY_RADIUS_TIME_FORMAT, KeyContainer.KEY_DPI_TIME_FORMAT, KeyContainer.KEY_IDC_DPI_TIME_FORMAT)
    //初始化必填String型配置项
    result.initRequiredTextValue(props, KeyContainer.KEY_APP_SPARK_PRE_MATCH_PARALLELISM, KeyContainer.KEY_APP_SPARK_PRE_MATCH_NAME, KeyContainer.KEY_APP_PRE_QUIET_EXIT_FILE_PATH, KeyContainer.KEY_APP_SPARK_FULL_MATCH_PARALLELISM, KeyContainer.KEY_APP_SPARK_FULL_MATCH_NAME, KeyContainer.KEY_APP_FULL_MATCH_RESULT_LOG_TYPE, KeyContainer.KEY_APP_FULL_QUIET_EXIT_FILE_PATH, KeyContainer.KEY_APP_SPARK_IDC_MATCH_PARALLELISM, KeyContainer.KEY_APP_SPARK_IDC_MATCH_NAME, KeyContainer.KEY_APP_IDC_MATCH_RESULT_LOG_TYPE, KeyContainer.KEY_APP_IDC_QUIET_EXIT_FILE_PATH, KeyContainer.KEY_APP_FINAL_SEPARATOR, KeyContainer.KEY_KAFKA_BROKER_LIST, KeyContainer.KEY_KAFKA_RESULT_TOPIC_NAME, KeyContainer.KEY_KAFKA_IDC_RESULT_TOPIC_NAME, KeyContainer.KEY_KAFKA_BATCH_COUNT, KeyContainer.KEY_HADOOP_NAT_DIR_PATH, KeyContainer.KEY_HADOOP_PRE_MATCH_DIR_PATH, KeyContainer.KEY_HADOOP_DPI_DIR_PATH, KeyContainer.KEY_HADOOP_IDC_DPI_DIR_PATH, KeyContainer.KEY_HADOOP_FILE_NAME_SEPARATOR, KeyContainer.KEY_H_BASE_HOST_MASTER, KeyContainer.KEY_H_BASE_ZK_QUORUM, KeyContainer.KEY_H_BASE_ZK_PORT, KeyContainer.KEY_H_BASE_RADIUS_TABLE_NAME_PREFIX, KeyContainer.KEY_H_BASE_FAMILY_NAME, KeyContainer.KEY_H_BASE_DETAIL_COLUMN_NAME, KeyContainer.KEY_RADIUS_FIELD_SEPARATOR, KeyContainer.KEY_RADIUS_ROW_KEY_SEPARATOR, KeyContainer.KEY_NAT_FIELD_SEPARATOR, KeyContainer.KEY_PRE_MATCH_FIELD_SEPARATOR, KeyContainer.KEY_DPI_FIELD_SEPARATOR, KeyContainer.KEY_IDC_DPI_FIELD_SEPARATOR)
    //初始化非必填Int型配置项
    result.initIntValue(props, KeyContainer.KEY_RADIUS_TIME_VALID_DIGITS, KeyContainer.KEY_RADIUS_TIME_RETAIN_DIGITS, KeyContainer.KEY_DPI_TIME_VALID_DIGITS, KeyContainer.KEY_DPI_TIME_RETAIN_DIGITS, KeyContainer.KEY_IDC_DPI_TIME_VALID_DIGITS, KeyContainer.KEY_IDC_DPI_TIME_RETAIN_DIGITS)
    //初始化必填Int型配置项
    result.initRequiredIntValue(props, KeyContainer.KEY_KAFKA_BATCH_COUNT, KeyContainer.KEY_HADOOP_PRE_MATCH_DIR_COUNT, KeyContainer.KEY_H_BASE_SCAN_CACHING_COUNT, KeyContainer.KEY_H_BASE_SCAN_BATCH_COUNT, KeyContainer.KEY_H_BASE_RADIUS_TABLE_COUNT,KeyContainer.KEY_RADIUS_END_TIME_INDEX, KeyContainer.KEY_NAT_START_TIME_INDEX, KeyContainer.KEY_NAT_END_TIME_INDEX, KeyContainer.KEY_NAT_PRIVATE_IP_INDEX, KeyContainer.KEY_PRE_MATCH_START_TIME_INDEX,KeyContainer.KEY_PRE_MATCH_END_TIME_INDEX, KeyContainer.KEY_DPI_START_TIME_INDEX, KeyContainer.KEY_DPI_END_TIME_INDEX, KeyContainer.KEY_IDC_DPI_PRIVATE_IP_INDEX, KeyContainer.KEY_IDC_DPI_START_TIME_INDEX)
    //初始化Int型数组配置项
    result.initIntArr(props, result.getTextByKey(KeyContainer.KEY_APP_FINAL_SEPARATOR), KeyContainer.KEY_RADIUS_NORMAL_MSG_INDEXES, KeyContainer.KEY_NAT_KEY_MSG_INDEXES, KeyContainer.KEY_NAT_NORMAL_MSG_INDEXES, KeyContainer.KEY_PRE_MATCH_KEY_MSG_INDEXES, KeyContainer.KEY_PRE_MATCH_NORMAL_MSG_INDEXES, KeyContainer.KEY_DPI_KEY_MSG_INDEXES, KeyContainer.KEY_DPI_NORMAL_MSG_INDEXES, KeyContainer.KEY_IDC_DPI_NORMAL_MSG_INDEXES)
    //初始化需要进行额外配置的配置项
    result.initTextValue(KeyContainer.KEY_HADOOP_PRE_MATCH_COMPRESSION_CODEC,HADOOP_COMPRESSION_CODEC_MAP(props.getProperty(KeyContainer.KEY_HADOOP_PRE_MATCH_COMPRESSION_TYPE,"gzip")))
    result.initIntValue(KeyContainer.KEY_HADOOP_NAT_BATCH_FILE_COUNT, props.getProperty(KeyContainer.KEY_HADOOP_NAT_BATCH_FILE_COUNT, "30").toInt)
    result.initIntValue(KeyContainer.KEY_HADOOP_NAT_DELAY_MILLIS, props.getProperty(KeyContainer.KEY_HADOOP_NAT_DELAY_MINUTES, "10").toInt * 60000)
    result.initIntValue(KeyContainer.KEY_HADOOP_DPI_BATCH_FILE_COUNT, props.getProperty(KeyContainer.KEY_HADOOP_DPI_BATCH_FILE_COUNT, "30").toInt)
    result.initIntValue(KeyContainer.KEY_HADOOP_DPI_DELAY_MILLIS, props.getProperty(KeyContainer.KEY_HADOOP_DPI_DELAY_MINUTES, "5").toInt * 60000)
    result.initIntValue(KeyContainer.KEY_HADOOP_DPI_LAG_MILLIS, props.getProperty(KeyContainer.KEY_HADOOP_DPI_LAG_SECONDS, "30").toInt * 1000)
    result.initIntValue(KeyContainer.KEY_HADOOP_IDC_DPI_BATCH_FILE_COUNT, props.getProperty(KeyContainer.KEY_HADOOP_IDC_DPI_BATCH_FILE_COUNT, "30").toInt)
    result.initIntValue(KeyContainer.KEY_HADOOP_IDC_DPI_DELAY_MILLIS, props.getProperty(KeyContainer.KEY_HADOOP_IDC_DPI_DELAY_MINUTES, "5").toInt * 60000)
    result.initIntValue(KeyContainer.KEY_HADOOP_PRE_MATCH_OVER_TIME_MILLIS, props.getProperty(KeyContainer.KEY_HADOOP_PRE_MATCH_OVER_TIME_MINUTES, "60").toInt * 60000)
    result.initIntValue(KeyContainer.KEY_HADOOP_PRE_MATCH_FORWARD_TIME_MILLIS, props.getProperty(KeyContainer.KEY_HADOOP_PRE_MATCH_FORWARD_TIME_MINUTES, "10").toInt * 60000)
    result.initIntValue(KeyContainer.KEY_HADOOP_PRE_MATCH_BACKWARD_TIME_MILLIS, props.getProperty(KeyContainer.KEY_HADOOP_PRE_MATCH_BACKWARD_TIME_MINUTES, "50").toInt * 60000)
    result
  }

}
