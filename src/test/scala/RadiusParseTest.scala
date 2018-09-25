import com.ais.common.LoggerObject
import com.ais.retain.creator.ObjectCreator

/** Created by ShiGZ on 2016/12/9. */
object RadiusParseTest extends LoggerObject {

  val testRadiusLog = "18725509723|1|100.126.209.199|0|0|0|0|1|2017-05-08 08:52:17|0|0|AHHF-MB0100000000000014e73d189506|2017-05-08 08:54:12"
  val testRadiusRowKey = "991.902.621.001|0007374024941|32790552781"


  def main(args: Array[String]) {
    try {
      val Array(settingPath): Array[String] = args
      val radiusParser = ObjectCreator.createRadiusParser(ObjectCreator.initSettings(settingPath))
      val radiusTupleOption = radiusParser.parse(testRadiusLog,testRadiusRowKey)
      val testRadiusArr = testRadiusLog.split('|')
      for (index <- testRadiusArr.indices) {
        print(index + ":" + testRadiusArr(index) + ";")
      }
      println()
      infoLog(radiusTupleOption)
    } catch {
      case ex: Exception =>
        errorLog(ex, ex.getCause)
    }
  }

}
