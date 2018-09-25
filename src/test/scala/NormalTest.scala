import com.ais.common.LoggerObject
import com.ais.common.tools.IPTool

/** Created by ShiGZ on 2017/5/9. */
object NormalTest extends LoggerObject {

  def main(args: Array[String]): Unit = {
    infoLog(IPTool.getHashValueByPartitionCount("46000|112.29.199.33|80|100.108.149.235",840))
//    infoLog(IPTool.getHashValueByPartitionCount("100.108.149.235",240),"100.108.149.235".reverse)
  }

}
