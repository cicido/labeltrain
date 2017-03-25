package wordseg

import common.{DXPUtils, SegWordUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by duanxiping on 2017/2/6.
  * 其他过滤逻辑在本文件进行,rule words 需要人工干预，并设定规则
  */
object FilterSegWord {
  val srcTable = "algo.dxp_label_word_seg"
  val dstTable = "algo.dxp_label_word_seg_filter"

  val wordNatureTable = "algo.dxp_label_word_nature"
  val ruleWordsTable = "algo.dxp_label_rulewords"

  val sparkEnv = new SparkEnv("SegWord")
  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val selectSQL = s"select id,words from ${srcTable} where stat_date=${dt}"
    // 进行规则过滤

    // 去掉频率少于3的词
    val stopSQL = "select distinct(word) from algo.dxp_label_word_nature where cnt < 3"
    val stopArr = sparkEnv.hiveContext.sql(stopSQL).map(_.getAs[String](0)).collect()

    // 去掉规则中use_flag标记为false的词
    val ruleSQL = "select distinct(word) from algo.dxp_label_word_rulewords " +
      " where use_flag = 0"
    val ruleArr = sparkEnv.hiveContext.sql(ruleSQL).map(_.getAs[String](0)).collect()

    val segRDD = sparkEnv.hiveContext.sql(selectSQL).repartition(200).map(r=>{
      val id = r.getAs[String](0)
      val wordsArr = r.getAs[String](1).split(",").filter(w=> {
        !stopArr.contains(w) && !ruleArr.contains(w)
      })
      (id,wordsArr)
    })

    // 保留过滤结果
    val segDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      segRDD.map(r=>{
        (r._1,r._2.mkString(","))
      }).toDF("id","words")
    }
    DXPUtils.saveDataFrame(segDF,dstTable,dt,sparkEnv.hiveContext)
  }
}
