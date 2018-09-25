import com.ais.common.LoggerObject
import com.ais.common.container.MultiContainer
import com.ais.common.tools.{HadoopTool, StringTool}
import com.ais.retain.container.KeyContainer
import com.ais.retain.service.full.FullMatchService
import org.apache.spark.SparkContext

/** Created by ShiGZ on 2017/5/11. */
object OnceFullMatchApp extends LoggerObject {

  def main(args: Array[String]): Unit = {
    val Array(settingPath) = args
    //获取sc和进行过广播的配置,供后续程序使用
    val (sc, settings) = FullMatchApp.getScAndSettingsByPath(settingPath)
    try {
      infoLog("程序启动成功!")
      val fullMatchService = FullMatchApp.getFullMatchService(settings)
      onceDpiMatch(sc, settings, fullMatchService)
      infoLog("程序退出运行!")
    } catch {
      case ex: Exception =>
        errorLog(ex, ex.getCause)
    } finally {
      sc.stop()
    }
  }

  def onceDpiMatch(sc: SparkContext, settings: MultiContainer, fullMatchService: FullMatchService): Unit = {
    val dpiDirPath = settings.getTextByKey(KeyContainer.KEY_HADOOP_DPI_DIR_PATH)
    val dpiFileNameAB = HadoopTool.getFileNameABFromPath(dpiDirPath)
    if (dpiFileNameAB.nonEmpty) {
      val dpiFileNameList = dpiFileNameAB.toList
      var dpiData = sc.emptyRDD[String]
      val batchCount = settings.getIntValueByKey(KeyContainer.KEY_HADOOP_DPI_BATCH_FILE_COUNT)
      for (index <- dpiFileNameList.indices) {
        try {
          dpiData = dpiData.union(sc.textFile(StringTool.joinValues(dpiDirPath, "/", dpiFileNameList.apply(index))))
          if ((index + 1) % batchCount == 0 || index == dpiFileNameList.length - 1) {
            infoLog("开始处理当前批 DPI 文件！")
            fullMatchService.doFullMatch(dpiData,0L, System.currentTimeMillis())
            dpiData = sc.emptyRDD[String]
            infoLog("当前批 DPI 文件处理结束！")
          }
        } catch {
          case ex: Exception =>
            errorLog(ex, "处理当前批 DPI 文件时出现了异常，异常信息为: ", ex.getCause)
        }
      }
    }
  }

}
