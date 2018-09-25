
package com.ais.common.tools.partitioner

import com.ais.common.tools.IPTool
import org.apache.spark.Partitioner


class IndexPartitioner(partitions: Int) extends Partitioner {
  def numPartitions = partitions

  def getPartition(key: Any): Int = {
    key match {
      case null => 0
      case iKey: Int => IPTool.getHashValueByPartitionCount(iKey, numPartitions)
      case textKey: String => IPTool.getHashValueByPartitionCount(textKey, numPartitions)
      case _ => 0
    }
  }

  override def equals(other: Any): Boolean = {
    other match {
      case h: IndexPartitioner =>
        h.numPartitions == numPartitions
      case _ =>
        false
    }
  }

}
