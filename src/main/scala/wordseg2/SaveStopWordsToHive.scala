package wordseg2

/**
  * Created by duanxiping on 2017/1/18.
  */
import common.DXPUtils
import org.apache.spark.sql.SQLContext


object SaveStopWordsToHive {
  val dstTable = "algo.dxp_label_stopwords"
  val sparkEnv = new SparkEnv("SaveStopWords")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val path = if (args(1).startsWith("/")) "file://" + args(1) else args(1)
    println(path)

    val wordsDF = {
      val sqlContext = SQLContext.getOrCreate(sparkEnv.sc)
      import sqlContext.implicits._
      sparkEnv.sc.wholeTextFiles(path).flatMap(r => {
        r._2.split("\n")
      }).toDF("word")
    }
    DXPUtils.saveDataFrame(wordsDF, dstTable, dt, sparkEnv.hiveContext)
  }
}
