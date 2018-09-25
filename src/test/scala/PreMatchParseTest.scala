import com.ais.common.LoggerObject
import com.ais.retain.creator.ObjectCreator

/** Created by ShiGZ on 2016/12/9. */
object PreMatchParseTest extends LoggerObject {

  val testPreMatchLog = "100.94.217.92|13941|183.60.15.198|443|1497432862000|1497432888000|100.94.217.92|13941|1497371144000|15155540993|1|221.130.118.41|AHHBE-M05221179601654ff808b099020"


  def main(args: Array[String]) {
    try {
      val Array(settingPath): Array[String] = args
      val preMatchParser = ObjectCreator.createPreMatchParser(ObjectCreator.initSettings(settingPath))
      val preMatchTupleOption = preMatchParser.parse(testPreMatchLog)
      val testPreMatchArr = testPreMatchLog.split('~')
      for (index <- testPreMatchArr.indices) {
        print(index + ":" + testPreMatchArr(index) + ";")
      }
      println()
      infoLog(preMatchTupleOption)
    } catch {
      case ex: Exception =>
        errorLog(ex, ex.getCause)
    }
  }

}
