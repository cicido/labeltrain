package wordseg

import common.DXPUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by duanxiping on 2017/2/6.
  */

object Word2Label {
  val srcTable = "algo.dxp_label_label2word"
  val desTable = "algo.dxp_label_word2label"

  val sparkEnv = new SparkEnv("Word2Label")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val outDt = args(1)

    val docsSQL = s"select classid, words from ${srcTable} where " +
      s"stat_date=${dt}"

    val docsRDD = sparkEnv.hiveContext.sql(docsSQL).repartition(200).flatMap(r => {
      val classid = r.getAs[Long](0)
      r.getAs[String](1).split(",").map(w=>{
        (w,Array(classid))
      })
    }).reduceByKey(_ ++ _)

    val trDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      docsRDD.map(r=>{
        (r._1,r._2.mkString(","))
      }).toDF("words","classids")
    }
    DXPUtils.saveDataFrame(trDF, desTable, outDt, sparkEnv.hiveContext)
  }
}
