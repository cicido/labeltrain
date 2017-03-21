package wordseg

import common.{DXPUtils, SegWordUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by duanxiping on 2017/2/6.
  */
object SegWord {
  val srcTable = "algo.dxp_label_corpus"
  val dstTable = "algo.dxp_label_word_seg"

  val wordNatureTable = "algo.dxp_label_word_nature"
  val sparkEnv = new SparkEnv("SegWord")
  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val out_dt = args(1)
    val selectSQL = s"select id,msg from ${srcTable}"
    // val corpusDF = sparkEnv.hiveContext.sql(selectSQL)
    // val segDF = segWordInDF(corpusDF, "msg","words")
    // 进行规则过滤

    // 停用词过滤
    val stopSQL = "select distinct(word) from algo.dxp_label_stopwords"
    val stopArr = sparkEnv.hiveContext.sql(stopSQL).map(_.getAs[String](0)).collect()

    val segRDD = sparkEnv.hiveContext.sql(selectSQL).repartition(200).map(r=>{
      val id = r.getAs[String](0)
      val msg = r.getAs[String](1)

      val wordsArr = SegWordUtils.segMsgWithNature(msg).filter(w=> {
        w._2.startsWith("n") && w._1.length > 1
      }).filterNot(w=>{(
          w._2.startsWith("nr") &&
            (Config.nrPrefix.filter(w._1.startsWith(_)).size != 0 ||
          Config.nrSuffix.filter(w._1.endsWith(_)).size != 0) ) ||
          (w._2.startsWith("nx") &&
            w._1.
              filterNot(""" ´~!\\\"″”#$&'()*+`./:;<>?@^[]{}_-|%,\\\\""".contains(_)).
              size < 2) || (
          w._1.startsWith("ns") && Config.nsStopWords.filter(w._1.endsWith(_)).size != 0
        )
      }).filter(w=>{
        !stopArr.contains(w._1) && !Config.nrStopWords.contains(w._1) &&
        !Config.otherStopWords.contains(w._1)
      })
      (id,msg,wordsArr)
    })
    segRDD.cache()

    // 保留词,词性及出现次数,以用于后续过滤,名词中有人名,地名,机构名及其他实体.
    // 地名与人名有大部分要过滤
    val wordDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      segRDD.flatMap(r=>{
        r._3.map(w=>{
          ((w._1,w._2),1)
        })
      }).reduceByKey(_ + _).
        map(r=>{
          (r._1._1,r._1._2,r._2)
        }).
        toDF("word", "nature","cnt")
    }
    DXPUtils.saveDataFrame(wordDF,wordNatureTable,out_dt,sparkEnv.hiveContext)

    // 保留文章编号与分词结果
    val segDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      segRDD.map(r=>{
        val words = r._3.map(w=>{
          w._1
        })
        (r._1,r._2,words.mkString(","))
      }).toDF("id","msg","words")
    }
    DXPUtils.saveDataFrame(segDF,dstTable,out_dt,sparkEnv.hiveContext)
  }
}
