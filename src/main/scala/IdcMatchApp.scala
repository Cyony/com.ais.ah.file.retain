import com.ais.common.LoggerObject
import com.ais.common.container.MultiContainer
import com.ais.common.tools.{FileTool, HadoopTool, StringTool}
import com.ais.retain.container.KeyContainer
import com.ais.retain.creator.ObjectCreator
import com.ais.retain.service.idc.IdcMatchService
import com.ais.retain.service.idc.converter.IdcDpiDetail
import com.ais.retain.service.pre.converter.RadiusDetail
import org.apache.spark.{SparkConf, SparkContext}

/** Created by ShiGZ on 2017/5/11. */
object IdcMatchApp extends LoggerObject {

  def main(args: Array[String]): Unit = {
    val Array(settingPath) = args
    val (sc, settings) = getScAndSettingsByPath(settingPath)
    try {
      infoLog("程序启动成功!")
      continuousFullMatch(sc, settings)
      infoLog("程序退出运行!")
    } catch {
      case ex: Exception =>
        errorLog(ex, ex.getCause)
    } finally {
      sc.stop()
    }
  }

  private def continuousFullMatch(sc: SparkContext, settings: MultiContainer): Unit = {
    val idcMatchService = ObjectCreator.createIdcMatchService(settings)
    val quietStopFilePath = settings.getTextByKey(KeyContainer.KEY_APP_IDC_QUIET_EXIT_FILE_PATH)
    while (!FileTool.isFileExist(quietStopFilePath)) {
      batchIdcDpiMatch(sc,settings,idcMatchService)
    }
    infoLog("检测到安全退出文件!即将停止运行!")
    FileTool.deleteFile(quietStopFilePath)
  }

  def batchIdcDpiMatch(sc: SparkContext, settings: MultiContainer, idcMatchService: IdcMatchService): Unit = {
    val idcDpiDirPath = settings.getTextByKey(KeyContainer.KEY_HADOOP_IDC_DPI_DIR_PATH)
    val idcDpiFileNameAB = HadoopTool.getFileNameABFromPathOfMaxCount(idcDpiDirPath, settings.getIntValueByKey(KeyContainer.KEY_HADOOP_IDC_DPI_BATCH_FILE_COUNT), settings.getTextByKey(KeyContainer.KEY_HADOOP_FILE_NAME_SEPARATOR), System.currentTimeMillis() - settings.getIntValueByKey(KeyContainer.KEY_HADOOP_IDC_DPI_DELAY_MILLIS))
    if (idcDpiFileNameAB.nonEmpty) {
      infoLog("开始处理当前批 IDC DPI 文件！")
      try {
        var idcDpiData = sc.emptyRDD[String]
        for (idcDpiFileName <- idcDpiFileNameAB) {
          idcDpiData = idcDpiData.union(sc.textFile(StringTool.joinValues(idcDpiDirPath, "/", idcDpiFileName)))
        }
        idcMatchService.doIdcMatch(idcDpiData)
      } catch {
        case ex: Exception =>
          errorLog(ex, "处理当前批 IDC DPI 文件时出现了异常，异常信息为: ", ex.getCause)
      } finally {
        for (idcDpiFileName <- idcDpiFileNameAB) {
          HadoopTool.deleteFile(idcDpiDirPath, idcDpiFileName)
        }
      }
      infoLog("当前批 IDC DPI 文件处理结束！")
    }
  }

  def getScAndSettingsByPath(settingPath: String): (SparkContext, MultiContainer) = {
    //从配置文件读取配置信息,并写入到container中
    val settings = ObjectCreator.initSettings(settingPath)
    val sparkConf = new SparkConf().setAppName(settings.getTextByKey(KeyContainer.KEY_APP_SPARK_IDC_MATCH_NAME))
    sparkConf.set("spark.default.parallelism", settings.getTextByKey(KeyContainer.KEY_APP_SPARK_IDC_MATCH_PARALLELISM)).set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").registerKryoClasses(Array(classOf[RadiusDetail],classOf[IdcDpiDetail]))

    //创建sc
    val sc = new SparkContext(sparkConf)
    (sc, sc.broadcast(settings).value)
  }

}
