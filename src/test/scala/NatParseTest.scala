import com.ais.common.LoggerObject
import com.ais.retain.creator.ObjectCreator

/** Created by ShiGZ on 2016/12/9. */
object NatParseTest extends LoggerObject {

  val testNatLog = "1494438426000#1494438437000#100.74.133.157#60130#112.28.132.79#54321#42.96.141.35#80#11"

  def main(args: Array[String]) {
    try {
      val Array(settingPath): Array[String] = args
      val testRadiusArr = testNatLog.split('#')
      for (index <- testRadiusArr.indices) {
        print(index + ":" + testRadiusArr(index) + ";")
      }
      println()
      val natParser = ObjectCreator.createNatParser(ObjectCreator.initSettings(settingPath))
      val natTupleOption = natParser.parse(testNatLog)
      if (natTupleOption.isDefined) {
        infoLog("解析结果为: ",natTupleOption.get)
      } else {
        infoLog("没有解析结果！")
      }
    } catch {
      case ex: Exception =>
        errorLog(ex, ex.getCause)
    }
  }

}


