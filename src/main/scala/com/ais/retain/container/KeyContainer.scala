package com.ais.retain.container

/** Created by ShiGZ on 2017/5/9. */
object KeyContainer {

  //应用必填配置 start
  val KEY_APP_SPARK_PRE_MATCH_PARALLELISM = "app.spark.pre.match.parallelism"
  val KEY_APP_SPARK_PRE_MATCH_NAME = "app.spark.pre.match.name"
  val KEY_APP_PRE_QUIET_EXIT_FILE_PATH = "app.pre.quiet.exit.file.path"
  val KEY_APP_SPARK_FULL_MATCH_PARALLELISM = "app.spark.full.match.parallelism"
  val KEY_APP_SPARK_FULL_MATCH_NAME = "app.spark.full.match.name"
  val KEY_APP_FULL_MATCH_RESULT_LOG_TYPE = "app.full.match.result.log.type"
  val KEY_APP_FULL_QUIET_EXIT_FILE_PATH = "app.full.quiet.exit.file.path"
  val KEY_APP_SPARK_IDC_MATCH_PARALLELISM = "app.spark.idc.parallelism"
  val KEY_APP_SPARK_IDC_MATCH_NAME = "app.spark.idc.match.name"
  val KEY_APP_IDC_MATCH_RESULT_LOG_TYPE = "app.idc.match.result.log.type"
  val KEY_APP_IDC_QUIET_EXIT_FILE_PATH = "app.idc.quiet.exit.file.path"
  val KEY_APP_FINAL_SEPARATOR = "app.final.separator"
  //应用必填配置 stop

  //kafka 必填配置 start
  val KEY_KAFKA_BROKER_LIST = "kafka.broker.list"
  val KEY_KAFKA_RESULT_TOPIC_NAME = "kafka.result.topic.name"
  val KEY_KAFKA_IDC_RESULT_TOPIC_NAME = "kafka.idc.result.topic.name"
  val KEY_KAFKA_BATCH_COUNT = "kafka.batch.count"
  //kafka 必填配置 stop

  //Hadoop 必填配置 start
  val KEY_HADOOP_NAT_DIR_PATH = "hadoop.nat.dir.path"
  val KEY_HADOOP_DPI_DIR_PATH = "hadoop.dpi.dir.path"
  val KEY_HADOOP_PRE_MATCH_DIR_PATH = "hadoop.pre.match.dir.path"
  val KEY_HADOOP_PRE_MATCH_DIR_COUNT = "hadoop.pre.match.dir.count"
  val KEY_HADOOP_PRE_MATCH_COMPRESSION_TYPE = "hadoop.pre.match.compression.type"
  val KEY_HADOOP_PRE_MATCH_COMPRESSION_CODEC = "hadoop.pre.match.compression.codec"
  val KEY_HADOOP_IDC_DPI_DIR_PATH = "hadoop.idc.dpi.dir.path"
  val KEY_HADOOP_FILE_NAME_SEPARATOR = "hadoop.file.name.separator"
  //Hadoop 必填配置

  //Hadoop 选填配置 start
  val KEY_HADOOP_NAT_BATCH_FILE_COUNT = "hadoop.nat.batch.file.count"
  val KEY_HADOOP_NAT_DELAY_MINUTES = "hadoop.nat.delay.minutes"
  val KEY_HADOOP_NAT_DELAY_MILLIS = "hadoop.nat.delay.millis"
  val KEY_HADOOP_DPI_BATCH_FILE_COUNT = "hadoop.dpi.batch.file.count"
  val KEY_HADOOP_DPI_DELAY_MINUTES = "hadoop.dpi.delay.minutes"
  val KEY_HADOOP_DPI_DELAY_MILLIS = "hadoop.dpi.delay.millis"
  val KEY_HADOOP_DPI_LAG_SECONDS = "hadoop.dpi.lag.seconds"
  val KEY_HADOOP_DPI_LAG_MILLIS = "hadoop.dpi.lag.millis"
  val KEY_HADOOP_IDC_DPI_BATCH_FILE_COUNT = "hadoop.idc.dpi.batch.file.count"
  val KEY_HADOOP_IDC_DPI_DELAY_MINUTES = "hadoop.idc.dpi.delay.minutes"
  val KEY_HADOOP_IDC_DPI_DELAY_MILLIS = "hadoop.idc.dpi.delay.millis"
  val KEY_HADOOP_PRE_MATCH_OVER_TIME_MINUTES = "hadoop.pre.match.over.time.minutes"
  val KEY_HADOOP_PRE_MATCH_OVER_TIME_MILLIS = "hadoop.pre.match.over.time.millis"
  val KEY_HADOOP_PRE_MATCH_FORWARD_TIME_MINUTES = "hadoop.pre.match.forward.time.minutes"
  val KEY_HADOOP_PRE_MATCH_FORWARD_TIME_MILLIS = "hadoop.pre.match.forward.time.millis"
  val KEY_HADOOP_PRE_MATCH_BACKWARD_TIME_MINUTES = "hadoop.pre.match.backward.time.minutes"
  val KEY_HADOOP_PRE_MATCH_BACKWARD_TIME_MILLIS = "hadoop.pre.match.backward.time.millis"
  //Hadoop 选填配置 stop

  //HBase 必填配置 start
  val KEY_H_BASE_HOST_MASTER = "hbase.host.master"
  val KEY_H_BASE_ZK_QUORUM = "hbase.zk.quorum"
  val KEY_H_BASE_ZK_PORT = "hbase.zk.port"
  val KEY_H_BASE_ROW_KEY_SEPARATOR = "hbase.rowkey.separator"
  val KEY_H_BASE_SCAN_CACHING_COUNT = "hbase.scan.caching.count"
  val KEY_H_BASE_SCAN_BATCH_COUNT = "hbase.scan.batch.count"
  val KEY_H_BASE_RADIUS_TABLE_NAME_PREFIX = "hbase.radius.table.name.prefix"
  val KEY_H_BASE_RADIUS_TABLE_COUNT = "hbase.radius.table.count"
  val KEY_H_BASE_FAMILY_NAME = "hbase.table.family.name"
  val KEY_H_BASE_DETAIL_COLUMN_NAME = "hbase.table.detail.column.name"
  //HBase 必填配置 stop

  //Radius 日志字段必填配置 start
  val KEY_RADIUS_FIELD_SEPARATOR = "radius.field.separator"
  val KEY_RADIUS_ROW_KEY_SEPARATOR = "radius.row.key.separator"
  val KEY_RADIUS_END_TIME_INDEX = "radius.end.time.index"
  val KEY_RADIUS_NORMAL_MSG_INDEXES = "radius.normal.msg.indexes"
  //Radius 日志字段必填配置 stop

  //Radius 日志可选字段配置 start
  val KEY_RADIUS_TIME_FORMAT = "radius.time.format"
  val KEY_RADIUS_TIME_VALID_DIGITS = "radius.time.valid.digits"
  val KEY_RADIUS_TIME_RETAIN_DIGITS = "radius.time.retain.digits"
  //Radius 日志可选字段配置 stop

  //NAT 日志字段必填配置 start
  val KEY_NAT_FIELD_SEPARATOR = "nat.field.separator"
  val KEY_NAT_START_TIME_INDEX = "nat.start.time.index"
  val KEY_NAT_END_TIME_INDEX = "nat.end.time.index"
  val KEY_NAT_PRIVATE_IP_INDEX = "nat.private.ip.index"
  val KEY_NAT_KEY_MSG_INDEXES = "nat.key.msg.indexes"
  val KEY_NAT_NORMAL_MSG_INDEXES = "nat.normal.msg.indexes"
  //NAT 日志字段必填配置 stop

  //NAT 日志可选字段配置 start
  val KEY_NAT_TIME_FORMAT = "nat.time.format"
  val KEY_NAT_TIME_VALID_DIGITS = "nat.time.valid.digits"
  val KEY_NAT_TIME_RETAIN_DIGITS = "nat.time.retain.digits"
  //NAT 日志可选字段配置 stop

  //DPI 选填字段 start
  val KEY_DPI_TIME_FORMAT = "dpi.time.format"
  val KEY_DPI_TIME_VALID_DIGITS = "dpi.time.valid.digits"
  val KEY_DPI_TIME_RETAIN_DIGITS = "dpi.time.retain.digits"
  //DPI 选填字段 stop

  //预匹配结果必填配置 start
  val KEY_PRE_MATCH_FIELD_SEPARATOR = "pre.match.field.separator"
  val KEY_PRE_MATCH_KEY_MSG_INDEXES = "pre.match.key.msg.indexes"
  val KEY_PRE_MATCH_START_TIME_INDEX = "pre.match.nat.start.time.index"
  val KEY_PRE_MATCH_END_TIME_INDEX = "pre.match.nat.end.time.index"
  val KEY_PRE_MATCH_NORMAL_MSG_INDEXES = "pre.match.normal.msg.indexes"
  //预匹配结果必填配置 stop

  //DPI 必填字段 start
  val KEY_DPI_FIELD_SEPARATOR = "dpi.field.separator"
  val KEY_DPI_START_TIME_INDEX = "dpi.start.time.index"
  val KEY_DPI_END_TIME_INDEX = "dpi.end.time.index"
  val KEY_DPI_KEY_MSG_INDEXES = "dpi.key.msg.indexes"
  val KEY_DPI_NORMAL_MSG_INDEXES = "dpi.normal.msg.indexes"
  //DPI 必填字段 stop

  //IDC DPI 日志字段必填配置 start
  val KEY_IDC_DPI_FIELD_SEPARATOR = "idc.dpi.field.separator"
  val KEY_IDC_DPI_PRIVATE_IP_INDEX = "idc.dpi.private.ip.index"
  //IDC DPI 日志字段必填配置 stop

  //IDC DPI 日志可选字段配置 start
  val KEY_IDC_DPI_TIME_FORMAT = "idc.dpi.time.format"
  val KEY_IDC_DPI_TIME_VALID_DIGITS = "idc.dpi.time.valid.digits"
  val KEY_IDC_DPI_TIME_RETAIN_DIGITS = "idc.dpi.time.retain.digits"
  val KEY_IDC_DPI_START_TIME_INDEX = "idc.dpi.start.time.index"
  val KEY_IDC_DPI_NORMAL_MSG_INDEXES = "idc.dpi.normal.msg.indexes"
  //IDC DPI 日志可选字段配置 stop

}