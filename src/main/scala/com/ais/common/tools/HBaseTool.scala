package com.ais.common.tools

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{Filter, FilterList}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD

/** Created by ShiGZ on 2016/8/10. */
object HBaseTool {

  /**
    * 初始化scan,添加默认配置.此时为扫描全表
    *
    * @return scan添加上默认的参数
    */
  def initialScan(cachingCount: Int, scanBatchCount: Int): Scan = {
    val result = new Scan()
    result.setMaxVersions()
    result.setCaching(cachingCount)
    result.setBatch(scanBatchCount)
    result
  }

  /**
    * 根据前缀列表,构建多前缀索引扫描器
    *
    * @param preFixList partition总数量
    * @return 多前缀索引扫描器
    */
  def multiScanWithPreFixList(cachingCount: Int, scanBatchCount: Int, preFixList: util.ArrayList[Filter]): Scan = {
    val result = initialScan(cachingCount, scanBatchCount)
    result.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE, preFixList))
    result
  }

  /**
    * HBase配置信息
    *
    * @param hBaseMaster       HBase的主机信息
    * @param quorumHBaseZK     HBase的zk主机信息
    * @param clientPortHBaseZK HBase的zk端口
    * @return HBase配置信息
    */
  def hBaseConfig(hBaseMaster: String, quorumHBaseZK: String, clientPortHBaseZK: String): Configuration = {
    val result = HBaseConfiguration.create()
    result.set("hbase.master", hBaseMaster)
    result.setInt("hbase.rpc.timeout", 900000)
    result.setInt("hbase.client.operation.timeout", 900000)
    result.setInt("hbase.client.scanner.timeout.period", 900000)
    result.set("hbase.zookeeper.quorum", quorumHBaseZK)
    result.set("hbase.zookeeper.property.clientPort", clientPortHBaseZK)

    result
  }

  /** 打印所有的表名
    *
    * @param hBaseConfig HBase配置信息
    */
  def listTableNames(hBaseConfig: Configuration): Unit = {
    val connection = ConnectionFactory.createConnection(hBaseConfig)
    val admin = connection.getAdmin
    try {
      val listTables: Array[TableName] = admin.listTableNames()
      listTables.foreach(println)
    } catch {
      case ex: Exception =>
        throw ex
    } finally {
      admin.close()
      connection.close()
    }
  }

  /** 根据表名删除表
    *
    * @param tableName   表名
    * @param hBaseConfig HBase配置信息
    */
  def deleteTable(tableName: String, hBaseConfig: Configuration): Unit = {
    val connection = ConnectionFactory.createConnection(hBaseConfig)
    val admin = connection.getAdmin
    try {
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table)) {
        admin.disableTable(table)
        admin.deleteTable(table)
      }
    } catch {
      case ex: Exception =>
        throw ex
    } finally {
      admin.close()
      connection.close()
    }
  }

  /** 初始化表,将表中内容清除
    *
    * @param tableName   表名
    * @param hBaseConfig HBase的配置信息
    */
  def initTable(tableName: String, hBaseConfig: Configuration): Unit = {
    val connection = ConnectionFactory.createConnection(hBaseConfig)
    val admin = connection.getAdmin
    try {
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table)) {
        if (admin.isTableDisabled(table)) {
          admin.enableTable(table)
        }
        admin.disableTable(table)
        admin.truncateTable(table, true)
      }
    } catch {
      case ex: Exception =>
        throw ex
    } finally {
      admin.close()
      connection.close()
    }
  }

  /** 确保表名存在
    *
    * @param tableName   表名
    * @param familyName  列簇名
    * @param hBaseConfig HBase的配置信息
    */
  def ensureTableExist(tableName: String, familyName: String, hBaseConfig: Configuration): Unit = {
    val connection = ConnectionFactory.createConnection(hBaseConfig)
    val admin = connection.getAdmin

    try {
      val table = TableName.valueOf(tableName)
      if (!admin.tableExists(table)) {
        val hTableDescriptor = new HTableDescriptor(table)
        hTableDescriptor.setDurability(Durability.ASYNC_WAL)
        val hColumnDescriptor = new HColumnDescriptor(familyName)
        hTableDescriptor.addFamily(hColumnDescriptor.setCompressionType(Algorithm.SNAPPY).setBloomFilterType
        (BloomType.ROW).setMaxVersions(1))
        admin.createTable(hTableDescriptor,  SplitKeyTool.calcSplitKeys())
      }
    } catch {
      case ex: Exception =>
        throw ex
    } finally {
      admin.close()
      connection.close()
    }
  }

}
