import com.ais.common.LoggerObject
import com.ais.retain.creator.ObjectCreator

/** Created by ShiGZ on 2017/5/9. */
object DpiParseTest extends LoggerObject {

//  val testDpiLog = "2017-06-07 16:50:54.797626000|2017-06-07 16:50:54.871682000|100.88.199.125|51931|112.29.152.167|80|112.29.152.167/live/5/60/17a59456f65b49c88f29f262130bf431.m3u|HTTP|0|0551||||Mozilla/4.0 (compatible; MS IE 6.0; (ziva))|200|1260|724"
  val testDpiLog = "2017-06-14 21:40:51.455000000|2017-06-14 21:40:56.583000000|100.103.182.27|42978|111.13.123.191|80|count.vrs.sohu.com/query/mix.action?partner=281&api_key=9854b2afa779e1a6bff1962447a09dbd&sver=6.6.0&ids=5703646%2C1278458%2C1%3B&sysver=5.1&plat=6&poid=1|HTTP|1|0551||Dalvik/2.1.0 (Linux; U; Android 5.1; OPPO R9m Build/LMY47I)|200|707|626"

  def main(args: Array[String]): Unit = {
    try {
      val Array(settingPath): Array[String] = args
      val dpiParser = ObjectCreator.createDpiParser(ObjectCreator.initSettings(settingPath))
      val dpiTupleOption = dpiParser.parse(testDpiLog)
      val testDpiArr = testDpiLog.split('|')
      for (index <- testDpiArr.indices) {
        print(index + ":" + testDpiArr(index) + ";")
      }
      println()
      infoLog(dpiTupleOption)
    } catch {
      case ex: Exception =>
        errorLog(ex, ex.getCause)
    }
  }

}
