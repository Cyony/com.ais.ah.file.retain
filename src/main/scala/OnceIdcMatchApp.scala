import com.ais.common.LoggerObject
import com.ais.common.container.MultiContainer
import com.ais.common.tools.{HadoopTool, StringTool}
import com.ais.retain.container.KeyContainer
import com.ais.retain.creator.ObjectCreator
import org.apache.spark.SparkContext

/** Created by ShiGZ on 2017/5/11. */
object OnceIdcMatchApp extends LoggerObject {

  def main(args: Array[String]): Unit = {
    val Array(settingPath) = args
    //获取sc和进行过广播的配置,供后续程序使用
    val (sc, settings) = IdcMatchApp.getScAndSettingsByPath(settingPath)
    try {
      infoLog("程序启动成功!")
      onceIdcDpiMatch(sc, settings)
      infoLog("程序退出运行!")
    } catch {
      case ex: Exception =>
        errorLog(ex, ex.getCause)
    } finally {
      sc.stop()
    }
  }

  def onceIdcDpiMatch(sc: SparkContext, settings: MultiContainer): Unit = {
    val idcMatchService = ObjectCreator.createIdcMatchService(settings)
    val idcDpiDirPath = settings.getTextByKey(KeyContainer.KEY_HADOOP_IDC_DPI_DIR_PATH)
    val idcDpiFileNameAB = HadoopTool.getFileNameABFromPath(idcDpiDirPath)
    if (idcDpiFileNameAB.nonEmpty) {
      val idcDpiFileNameList = idcDpiFileNameAB.toList
      var idcDpiData = sc.emptyRDD[String]
      val batchCount = settings.getIntValueByKey(KeyContainer.KEY_HADOOP_IDC_DPI_BATCH_FILE_COUNT)
      for (index <- idcDpiFileNameList.indices) {
        try {
          idcDpiData = idcDpiData.union(sc.textFile(StringTool.joinValues(idcDpiDirPath, "/", idcDpiFileNameList.apply(index))))
          if ((index + 1) % batchCount == 0 || index == idcDpiFileNameList.length - 1) {
            infoLog("开始处理当前批 IDC DPI 文件！")
            idcMatchService.doIdcMatch(idcDpiData)
            idcDpiData = sc.emptyRDD[String]
            infoLog("当前批 IDC DPI 文件处理结束！")
          }
        } catch {
          case ex: Exception =>
            errorLog(ex, "处理当前批 IDC DPI 文件时出现了异常，异常信息为: ", ex.getCause)
        } finally {
        }
      }
    }
  }

}
