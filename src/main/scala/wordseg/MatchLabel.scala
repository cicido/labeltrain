package wordseg

import common.DXPUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.collection.JavaConversions._

/**
  * Created by duanxiping on 2017/2/6.
  */
object MatchLabel {
  val srcTable = "uxip.sdl_uxip_search_keyword_d"
  val dstTable = "algo.dxp_label_imei_label"
  val sparkEnv = new SparkEnv("MatchLabel")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    //搜狗词典数据放在20170401这个分区中.
    val out_dt = 20170401

    val idLableMap = sparkEnv.sc.textFile("/tmp/duanxiping/id2label.txt").map(r=>{
      r.trim.split(",")
    }).filter(_.length ==2).map(r=>(r(0).toInt,r(1))).collectAsMap()

    // 只保留两级
    val classMap = sparkEnv.sc.textFile("/tmp/duanxiping/class.txt").map(r=>{
      r.trim.split(",")
    }).filter(_.length>1).map(r=>{
      val id = r(1).toInt
      val name = idLableMap(r(0).toInt) + "-" + idLableMap(r(1).toInt)
      (id,name)
    }).collectAsMap()

    val wordsLabelSQL = s"select word,s_id from algo.dxp_label_sougowords " +
      s"where stat_date=${out_dt}"
    val wordLabelMap = sparkEnv.hiveContext.sql(wordsLabelSQL).map(r => {
      (r.getAs[String](0), Set(r.getAs[Int](1)))
    }).reduceByKey(_ ++ _).map(r=>(r._1,r._2.toArray)).collectAsMap()

    println("*"*40)
    println("wordLableMap size:" + wordLabelMap.size)
    wordLabelMap.toArray.take(20).foreach(r=>{
      println(r._1 +"\t" + r._2.mkString(","))
    })

    println("*"*10 + "分词测试" + "*"*10)
    DXPUtils.segMsgWithNature("我是中国人").foreach(r=>{
      println(r._1+"/"+r._2)
    })

    val searchSQL = s"select imei,key_word from ${srcTable} " +
      s"where stat_date=${dt} limit 10000"
    val brWordLabelMap = sparkEnv.sc.broadcast(wordLabelMap)
    //先按逗号分割，再分词，统计词的个数，再查找词对应的类别，合并所有类别及次数
    val searchDF = sparkEnv.hiveContext.sql(searchSQL).repartition(300).map(r => {
      val imei = r.getAs[String](0)
      val key_word = r.getAs[String](1)
      val words = key_word.split(",").flatMap(d => {
        DXPUtils.segMsgWithNature(d).filter(w => {
          w._2.startsWith("n") && w._1.length > 1 && brWordLabelMap.value.contains(w._1)
        })
      })
      println("xx"*20)
      println(words.map(_._1).mkString(","))

      val classids = words.
        map(w=>(w._1, 1)).groupBy(_._1).
        map(l => (l._1, l._2.map(_._2).reduce(_ + _))).toArray./*将map类型转成Array类型,避免flatMap出错*/
        flatMap(w => {
          (brWordLabelMap.value)(w._1).map(x => (x,w._2))
        }).groupBy(_._1).map(l => (l._1, l._2.map(_._2).reduce(_ + _))).
        toArray.sortWith(_._2 > _._2).take(5).map(_._1)
      val classnames = classids.map(w=>classMap(w))
      (imei,key_word,words.mkString(","), classids.mkString(","),classnames.mkString(","))
    })

    val trDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      searchDF.toDF("imei","key_word","words","classids","classnames")
    }
    DXPUtils.saveDataFrame(trDF, dstTable, dt, sparkEnv.hiveContext)
  }
}
