package wordseg

/**
  * Created by duanxiping on 2017/2/6.
  */

import common.DXPUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object GetDocLabel {
  val srcTable = "algo.dxp_label_subject_words"
  val desTable = "algo.dxp_label_doc2Label"

  val sparkEnv = new SparkEnv("GetDocLabel")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val outDt = args(1)
    val msgSQL = s"select id,msg,words from algo.dxp_label_word_seg"
    val msgDF = sparkEnv.hiveContext.sql(msgSQL)

    val docsSQL = s"select id from ${srcTable} where " +
      s"stat_date=${dt}"
    val docsRDD = sparkEnv.hiveContext.sql(docsSQL).repartition(100).map(r => {
      r.getAs[String](0)
    }).zipWithUniqueId().flatMap(r=>{
      r._1.split("\\|").map(d=>{
        (r._2, d)
      })
    })

    val trDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      docsRDD.toDF("classid", "id")
    }.join(msgDF,"id")

    DXPUtils.saveDataFrame(trDF, desTable, outDt, sparkEnv.hiveContext)
  }
}
