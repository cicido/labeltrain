package wordseg

import common.DXPUtils
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
    val nrStopWords = Array("先生","女士","大爷","小姐","某")
    val nsStopWords = Array("路","市","区","县","省","镇","村","乡")

    // 停用词过滤
    val stopSQL = "select distinct(word) from algo.dxp_label_stopwords"
    val stopArr = sparkEnv.hiveContext.sql(stopSQL).map(_.getAs[String](0)).collect()

    val segRDD = sparkEnv.hiveContext.sql(selectSQL).repartition(200).map(r=>{
      val id = r.getAs[String](0)
      val msg = r.getAs[String](1)
      val wordsArr = DXPUtils.segMsgWithNature(msg).filter(r=>{
        r._2.startsWith("n") && r._1.length > 1 && ((
          !r._2.startsWith("nx") &&
          nrStopWords.filter(r._1.contains(_)).size == 0 &&
          nsStopWords.filter(r._1.endsWith(_)).size == 0 ) ||
          (r._2.startsWith("nx") &&
            r._1.
              filterNot(""" ´~!\\\"″”#$&'()*+`./:;<>?@^[]{}_-|%,\\\\""".contains(_)).
              size > 1))
      }).filter(r=>{
        !stopArr.contains(r._1)
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
