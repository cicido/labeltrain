package wordseg

import common.DXPUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by duanxiping on 2017/2/6.
  */

object Label2WordWithFilter {
  val srcTable = "algo.dxp_label_subject_words_with_blas"
  val desTable = "algo.dxp_label_label2word"

  val sparkEnv = new SparkEnv("Label2Word")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val outDt = args(1)

    /*
    val stopsWordsSQL = "select words from algo.dxp_label_word2label " +
      "where stat_date = 20170222 and size(split(classids,',')) > 200"
    val stopsWordsArr = sparkEnv.hiveContext.sql(stopsWordsSQL).map(r=>{
      r.getAs[String](0)
    }).collect()
    println("*\n"*30)
    println(stopsWordsArr.length)
    */

    val filterSQL = s"select distinct(word) from algo.dxp_label_rulewords " +
      s"where use_flag = 0"
    val filterArr =sparkEnv.hiveContext.sql(filterSQL).map(r=>{
      r.getAs[String](0)
    }).collect()

    val msgSQL = s"select id,words from algo.dxp_label_docvec_with_blas where " +
      s"stat_date=${outDt}"

    //问题：执行时间过长
    val idMsgMap = sparkEnv.hiveContext.sql(msgSQL).repartition(200).map(r=>{
      val wordsStr = r.getAs[String](1).split(",").
        filterNot(filterArr.contains(_)).mkString(",")
      (r.getAs[String](0),wordsStr)
    }).collectAsMap()

    val docsSQL = s"select id from ${srcTable} where " +
      s"stat_date=${dt}"

    val brIdWordsMap = sparkEnv.sc.broadcast(idMsgMap)
    val docsRDD = sparkEnv.hiveContext.sql(docsSQL).repartition(200).rdd.filter(r=>{
      r.getAs[String](0).split("\\|").length > 100
    }).map(r => {
      val ids = r.getAs[String](0)
      val words = ids.split("\\|").map(d => {
        val idWordsMap = brIdWordsMap.value
        try {
          idWordsMap(d)
        }catch {
          case e:Exception => {
            println(s"can not find $d")
            ""
          }
        }
      }).mkString(",").split(",").map(x=>{
        (x,1)
      }).groupBy(_._1).
        map(l => (l._1, l._2.map(_._2).reduce(_+_))).toArray.
        sortWith(_._2>_._2).map(x=>x._1+";"+x._2).mkString(",")
      (ids,words)
    })

    val trDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      docsRDD.zipWithUniqueId().map(r=>{
        (r._2,r._1._1,r._1._2)
      }).toDF("classid","ids","words")
    }
    DXPUtils.saveDataFrame(trDF, desTable, outDt, sparkEnv.hiveContext)
  }
}
