import com.ais.common.LoggerObject
import com.ais.retain.creator.ObjectCreator

/** Created by ShiGZ on 2017/5/9. */
object IdcDpiParseTest extends LoggerObject {

  val testIdcDpiLog = "2017-06-14 21:40:51.455000000|2017-06-14 21:40:56.583000000|100.103.182.27|42978|111.13.123.191|80|count.vrs.sohu.com/query/mix.action?partner=281&api_key=9854b2afa779e1a6bff1962447a09dbd&sver=6.6.0&ids=5703646%2C1278458%2C1%3B&sysver=5.1&plat=6&poid=1|HTTP|1|0551||Dalvik/2.1.0 (Linux; U; Android 5.1; OPPO R9m Build/LMY47I)|200|707|626"



  def main(args: Array[String]): Unit = {
    try {
      val Array(settingPath): Array[String] = args
      val idcDpiParser = ObjectCreator.createIdcDpiParser(ObjectCreator.initSettings(settingPath))
      val idcDpiTupleOption = idcDpiParser.parse(testIdcDpiLog)
      val testIdcDpiArr = testIdcDpiLog.split('|')
      for (index <- testIdcDpiArr.indices) {
        print(index + ":" + testIdcDpiArr(index) + ";")
      }
      println()
      infoLog(idcDpiTupleOption)
    } catch {
      case ex: Exception =>
        errorLog(ex, ex.getCause)
    }
  }

}
