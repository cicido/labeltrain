package wordseg2

/**
  * Created by duanxiping on 2017/1/18.
  */
import common.DXPUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._



object SaveFileDataToHive {
  /*
  var hiveContext: HiveContext = null
  var sc: SparkContext = null
  var sqlContext: SQLContext = null
  */

  val dstTable = "algo.dxp_label_corpus"

  def main(args: Array[String]): Unit = {
    //initSpark("SaveCorpusData")
    val sparkEnv = new SparkEnv("SaveCorpusData")
    val dt = args(0)
    val path = if (args(1).startsWith("/")) "file://" + args(1) else args(1)
    println(path)
    val prefix = args(2)

    val rdd = sparkEnv.sc.wholeTextFiles(path)
    println(rdd.count)
    val text = rdd.flatMap(r => {
      r._2.split("\n")
    }).filter(_.length > 100).zipWithUniqueId()

    val cols = Array(("id", "string"), ("msg", "string"))
    val fields = cols.map(f =>
      f._2 match {
        case "string" => StructField(f._1, StringType, nullable = false)
        case "int" => StructField(f._1, IntegerType, nullable = false)
        case "double" => StructField(f._1, DoubleType, nullable = false)
        case "long" => StructField(f._1, LongType, nullable = false)
      })
    val schema = StructType(fields)
    val newDF = sparkEnv.hiveContext.createDataFrame(text.map(r => Row(prefix + r._2, r._1)), schema)
    DXPUtils.saveDataFrame(newDF, dstTable, dt, sparkEnv.hiveContext)
  }

  /*
  def initSpark(appname: String): Unit = {
    /*
    System.setProperty("user.name", "ad_recommend")
    System.setProperty("HADOOP_USER_NAME", "ad_recommend")
    */
    val sparkConf: SparkConf = new SparkConf().setAppName(appname)
    sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("mapred.output.compress", "false")
    hiveContext = new HiveContext(sc)
    /*
    hiveContext.setConf("mapred.output.compress", "false")
    hiveContext.setConf("hive.exec.compress.output", "false")
    hiveContext.setConf("mapreduce.output.fileoutputformat.compress", "false")
    */
    sqlContext = new SQLContext(sc)
  }
  */

}
