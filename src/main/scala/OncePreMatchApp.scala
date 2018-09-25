import com.ais.common.LoggerObject
import com.ais.common.container.MultiContainer
import com.ais.common.tools.{HadoopTool, StringTool}
import com.ais.retain.container.KeyContainer
import com.ais.retain.service.pre.PreMatchService
import org.apache.spark.SparkContext

/** Created by ShiGZ on 2017/5/10. */
object OncePreMatchApp extends LoggerObject {

  def main(args: Array[String]): Unit = {
    val Array(settingPath) = args
    //获取sc和进行过广播的配置,供后续程序使用
    val (sc, settings) = PreMatchApp.getScAndSettingsByPath(settingPath)
    try {
      infoLog("程序启动成功!")
      val preMatchService = PreMatchApp.getPreMatchService(settings)
      onceNatMatch(sc, settings, preMatchService)
      infoLog("程序退出运行!")
    } catch {
      case ex: Exception =>
        errorLog(ex, ex.getCause)
    } finally {
      sc.stop()
    }
  }

  def onceNatMatch(sc: SparkContext, settings: MultiContainer, preMatchService: PreMatchService): Unit = {
    val natDirPath = settings.getTextByKey(KeyContainer.KEY_HADOOP_NAT_DIR_PATH)
    val natFileNameAB = HadoopTool.getFileNameABFromPath(natDirPath)
    if (natFileNameAB.nonEmpty) {
      val natFileNameList = natFileNameAB.toList
      var natData = sc.emptyRDD[String]
      val batchCount = settings.getIntValueByKey(KeyContainer.KEY_HADOOP_NAT_BATCH_FILE_COUNT)
      for (index <- natFileNameList.indices) {
        try {
          natData = natData.union(sc.textFile(StringTool.joinValues(natDirPath, "/", natFileNameList.apply(index))))
          if ((index + 1) % batchCount == 0 || index == natFileNameList.length - 1) {
            infoLog("开始处理当前批 NAT 文件！")
            preMatchService.doPreMatch(natData)
            natData = sc.emptyRDD[String]
            infoLog("当前批 NAT 文件处理结束！")
          }
        } catch {
          case ex: Exception =>
            errorLog(ex, "处理当前批 NAT 文件时出现了异常，异常信息为: ", ex.getCause)
        }
      }
    }
  }

}
