import com.ais.common.LoggerObject
import com.ais.common.container.MultiContainer
import com.ais.common.tools.{FileTool, HadoopTool, StringTool}
import com.ais.retain.container.KeyContainer
import com.ais.retain.creator.ObjectCreator
import com.ais.retain.service.pre.PreMatchService
import com.ais.retain.service.pre.converter.{NatDetail, RadiusDetail}
import org.apache.spark.{SparkConf, SparkContext}

/** Created by ShiGZ on 2017/5/10. */
object PreMatchApp extends LoggerObject {

  def main(args: Array[String]): Unit = {
    val Array(settingPath) = args
    val (sc, settings) = getScAndSettingsByPath(settingPath)
    try {
      infoLog("程序启动成功!")
      continuousPreMatch(sc, settings)
      infoLog("程序退出运行!")
    } catch {
      case ex: Exception =>
        errorLog(ex, ex.getCause)
    } finally {
      sc.stop()
    }
  }

  private def continuousPreMatch(sc: SparkContext, settings: MultiContainer): Unit = {
    val preMatchService = getPreMatchService(settings)
    val quietStopFilePath = settings.getTextByKey(KeyContainer.KEY_APP_PRE_QUIET_EXIT_FILE_PATH)
    while (!FileTool.isFileExist(quietStopFilePath)) {
      batchNatMatch(sc,settings,preMatchService)
    }
    infoLog("检测到安全退出文件!即将停止运行!")
    FileTool.deleteFile(quietStopFilePath)
  }

  def batchNatMatch(sc: SparkContext, settings: MultiContainer, preMatchService: PreMatchService): Unit = {
    val natDirPath = settings.getTextByKey(KeyContainer.KEY_HADOOP_NAT_DIR_PATH)
    val natFileNameAB = HadoopTool.getFileNameABFromPathOfMaxCount(natDirPath, settings.getIntValueByKey(KeyContainer.KEY_HADOOP_NAT_BATCH_FILE_COUNT), settings.getTextByKey(KeyContainer.KEY_HADOOP_FILE_NAME_SEPARATOR), System.currentTimeMillis() - settings.getIntValueByKey(KeyContainer.KEY_HADOOP_NAT_DELAY_MILLIS))
    if (natFileNameAB.nonEmpty) {
      infoLog("开始处理当前批 Nat 文件！")
      try {
        var natData = sc.emptyRDD[String]
        for (natFileName <- natFileNameAB) {
          natData = natData.union(sc.textFile(StringTool.joinValues(natDirPath, "/", natFileName)))
        }
        preMatchService.doPreMatch(natData)
      } catch {
        case ex: Exception =>
          errorLog(ex, "处理当前批 Nat 文件时出现了异常，异常信息为: ", ex.getCause)
      } finally {
        for (natFileName <- natFileNameAB) {
          HadoopTool.deleteFile(natDirPath, natFileName)
        }
      }
      infoLog("当前批 Nat 文件处理结束！")
    }
  }

  def getScAndSettingsByPath(settingPath: String): (SparkContext, MultiContainer) = {
    //从配置文件读取配置信息,并写入到container中
    val settings = ObjectCreator.initSettings(settingPath)
    val sparkConf = new SparkConf().setAppName(settings.getTextByKey(KeyContainer.KEY_APP_SPARK_PRE_MATCH_NAME))
    sparkConf.set("spark.default.parallelism", settings.getTextByKey(KeyContainer.KEY_APP_SPARK_PRE_MATCH_PARALLELISM))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").registerKryoClasses(Array(classOf[RadiusDetail], classOf[NatDetail]))

    //创建sc
    val sc = new SparkContext(sparkConf)
    (sc, sc.broadcast(settings).value)
  }

  def getPreMatchService(settings: MultiContainer): PreMatchService = {
    ObjectCreator.createPreMatchService(settings)
  }

}
