import com.ais.common.LoggerObject
import com.ais.common.container.MultiContainer
import com.ais.common.tools.{FileTool, HadoopTool, StringTool}
import com.ais.retain.container.KeyContainer
import com.ais.retain.creator.ObjectCreator
import com.ais.retain.service.full.FullMatchService
import com.ais.retain.service.full.converter.{DpiDetail, PreMatchDetail}
import org.apache.spark.{SparkConf, SparkContext}

/** Created by ShiGZ on 2017/5/11. */
object FullMatchApp extends LoggerObject {

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
    val fullMatchService = getFullMatchService(settings)
    val quietStopFilePath = settings.getTextByKey(KeyContainer.KEY_APP_FULL_QUIET_EXIT_FILE_PATH)
    while (!FileTool.isFileExist(quietStopFilePath)) {
      batchDpiMatch(sc,settings,fullMatchService)
      deleteOverTimePreMatchFiles(sc,settings)
    }
    infoLog("检测到安全退出文件!即将停止运行!")
    FileTool.deleteFile(quietStopFilePath)
  }

  def deleteOverTimePreMatchFiles(sc: SparkContext, settings: MultiContainer): Unit = {
    sc.parallelize(1 to settings.getIntValueByKey(KeyContainer.KEY_HADOOP_PRE_MATCH_DIR_COUNT),settings.getIntValueByKey(KeyContainer.KEY_HADOOP_PRE_MATCH_DIR_COUNT)).mapPartitionsWithIndex((index,iterator) => {
      val rootDirPath = StringTool.joinValues(settings.getTextByKey(KeyContainer.KEY_HADOOP_PRE_MATCH_DIR_PATH), "/", index)
      val fileNameList = HadoopTool.getFileNameABFromPath(rootDirPath)
      val overTimeMillis = System.currentTimeMillis() - settings.getIntValueByKey(KeyContainer.KEY_HADOOP_PRE_MATCH_OVER_TIME_MILLIS)
      for (fileName <- fileNameList) {
        if (overTimeMillis >= fileName.toLong) {
          HadoopTool.deleteFile(rootDirPath,fileName)
        }
      }
      iterator
    }).count()
  }

  def batchDpiMatch(sc: SparkContext, settings: MultiContainer, fullMatchService: FullMatchService): Unit = {
    val dpiDirPath = settings.getTextByKey(KeyContainer.KEY_HADOOP_DPI_DIR_PATH)
    val (latestFileCollectTime,dpiFileNameList) = HadoopTool.getFileNameListAndPeriodsFromPathOfMaxCount(dpiDirPath, settings.getIntValueByKey(KeyContainer.KEY_HADOOP_DPI_BATCH_FILE_COUNT), settings.getTextByKey(KeyContainer.KEY_HADOOP_FILE_NAME_SEPARATOR), System.currentTimeMillis() - settings.getIntValueByKey(KeyContainer.KEY_HADOOP_DPI_DELAY_MILLIS))
    if (dpiFileNameList.nonEmpty) {
      infoLog("开始处理当前批 DPI 文件！")
      try {
        var dpiData = sc.emptyRDD[String]
        for (dpiFileName <- dpiFileNameList) {
          dpiData = dpiData.union(sc.textFile(StringTool.joinValues(dpiDirPath, "/", dpiFileName)))
        }
        fullMatchService.doFullMatch(dpiData,latestFileCollectTime + settings.getIntValueByKey(KeyContainer.KEY_HADOOP_DPI_DELAY_MILLIS) - settings.getIntValueByKey(KeyContainer.KEY_HADOOP_PRE_MATCH_BACKWARD_TIME_MILLIS),latestFileCollectTime + settings.getIntValueByKey(KeyContainer.KEY_HADOOP_DPI_DELAY_MILLIS) + settings.getIntValueByKey(KeyContainer.KEY_HADOOP_PRE_MATCH_FORWARD_TIME_MILLIS))
      } catch {
        case ex: Exception =>
          errorLog(ex, "处理当前批 DPI 文件时出现了异常，异常信息为: ", ex.getCause)
      } finally {
        for (dpiFileName <- dpiFileNameList) {
          HadoopTool.deleteFile(dpiDirPath, dpiFileName)
        }
      }
      infoLog("当前批 DPI 文件处理结束！")
    }
  }

  def getScAndSettingsByPath(settingPath: String): (SparkContext, MultiContainer) = {
    //从配置文件读取配置信息,并写入到container中
    val settings = ObjectCreator.initSettings(settingPath)
    val sparkConf = new SparkConf().setAppName(settings.getTextByKey(KeyContainer.KEY_APP_SPARK_FULL_MATCH_NAME))
    sparkConf.set("spark.default.parallelism", settings.getTextByKey(KeyContainer.KEY_APP_SPARK_FULL_MATCH_PARALLELISM))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").registerKryoClasses(Array(classOf[PreMatchDetail],classOf[DpiDetail]))

    //创建sc
    val sc = new SparkContext(sparkConf)
    (sc, sc.broadcast(settings).value)
  }

  def getFullMatchService(settings: MultiContainer): FullMatchService = {
    ObjectCreator.createFullMatchService(settings)
  }

}
