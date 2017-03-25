package wordseg2

import common.DXPUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by duanxiping on 2017/2/6.
  */
object MatchLabel {
  val srcTable = "uxip.sdl_uxip_search_keyword_d"
  val dstTable = "algo.dxp_label_imei_label"
  val sparkEnv = new SparkEnv("MatchLabel")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val out_dt = args(1)

    val wordsLabelSQL = s"select words,classids from algo.dxp_label_word2label"
    val wordLabelMap = sparkEnv.hiveContext.sql(wordsLabelSQL).map(r => {
      (r.getAs[String](0), r.getAs[String](1).split(","))
    }).filter(r=>{
      r._2.length < 400
    }).collectAsMap()


    val searchSQL = s"select imei,key_word from ${srcTable} " +
      s"where stat_date=${dt}"
    val brWordLabelMap = sparkEnv.sc.broadcast(wordLabelMap)
    //先按逗号分割，再分词，统计词的个数，再查找词对应的类别，合并所有类别及次数
    val searchDF = sparkEnv.hiveContext.sql(searchSQL).repartition(300).map(r => {
      val wordLabelMap = brWordLabelMap.value
      val imei = r.getAs[String](0)
      val classids = r.getAs[String](1).split(",").flatMap(d => {
        DXPUtils.segMsgWithNature(d).filter(w => {
          w._2.startsWith("n")
        })
      }).map(d => (d._1, 1)).groupBy(_._1).
        map(l => (l._1, l._2.map(_._2).reduce(_ + _))).
        flatMap(w => {
          if (wordLabelMap.contains(w._1))
            wordLabelMap(w._1).map(x => (x, w._2))
          else
            Array[(String, Int)]()
        }).groupBy(_._1).map(l => (l._1, l._2.map(_._2).reduce(_ + _))).
        toArray.sortWith(_._2 > _._2).take(5).map(t => t._1 + "|" + t._2).mkString(",")
      (imei, classids)
    })

    val trDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      searchDF.toDF("imei", "classids")
    }

    DXPUtils.saveDataFrame(trDF, dstTable, out_dt, sparkEnv.hiveContext)
  }
}
