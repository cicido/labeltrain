package wordseg

/**
  * Created by duanxiping on 2017/1/18.
  */
import common.DXPUtils
import org.apache.spark.sql.SQLContext


object SaveRuleWordsToHive {
  val dstTable = "algo.dxp_label_rulewords"
  val sparkEnv = new SparkEnv("SaveRuleWords")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val path = if (args(1).startsWith("/")) "file://" + args(1) else args(1)
    println(path)

    val wordsDF = {
      val sqlContext = SQLContext.getOrCreate(sparkEnv.sc)
      import sqlContext.implicits._
      sparkEnv.sc.wholeTextFiles(path).flatMap(r => {
        r._2.split("\n")
      }).filter(_.split(" ").length == 3).map(r=>{
        val arr = r.split(" ")
        (arr(0),arr(1),arr(2).toInt)
      }).toDF("word","nature","use_flag")
    }
    DXPUtils.saveDataFrame(wordsDF, dstTable, dt, sparkEnv.hiveContext)
  }
}
