package wordseg

/**
  * Created by duanxiping on 2017/2/6.
  */

import common.DXPUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object CreateDocVec {
  val srcTable = "algo.dxp_label_word_seg"
  val desTable = "algo.dxp_label_docvec"

  val sparkEnv = new SparkEnv("CreateDocVec")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val out_dt = args(1)
    val selectSQL = s"select id, words from ${srcTable} where stat_date=${dt}"
    val docDF = sparkEnv.hiveContext.sql(selectSQL)

    val wordsSQL = s"select a.word,a.vecstring from algo.dxp_label_word_vec a " +
      s"join (select word,trval from algo.dxp_label_textrank_words where " +
      s"stat_date=${dt} order by trval desc limit 300000) b " +
      s"on a.word = b.word where a.stat_date=${dt}"

    val stopWordsSQL = s"select distinct(word) from algo.dxp_label_stopwords"
    val selfDefinedStopWords = Array("先生","女士","大爷","小姐","某")
    val stopWordsArr = sparkEnv.hiveContext.sql(stopWordsSQL).
      map(_.getAs[String](0)).collect()



    val word2vecMap = sparkEnv.hiveContext.sql(wordsSQL).map(r => {
      val word = r.getAs[String](0)
      val vec = r.getAs[String](1).split(",").map(_.toDouble)
      //val wordVec = Vectors.dense(vec)
      (word, vec)
    }).collect().filterNot(r=>{
      stopWordsArr.contains(r._1) ||
      selfDefinedStopWords.filter(w=>r._1.contains(w)).length > 0
    }).toMap

    println("*\n" * 30)
    println(word2vecMap.size)
    word2vecMap.foreach(r=>println(r._1))

    val docVecRDD = docDF.repartition(200).map(r => {
      val id = r.getAs[String](0)
      val words = r.getAs[String](1).split(",").filter(word2vecMap.contains(_))
      (id,words)
    }).filter(_._2.length > 5).map(r=>{
      val oriDocVec = r._2.map(word2vecMap(_)).reduceLeft((a, b) => {
        a.zip(b).map(r => r._1 + r._2)
      })
      //归一化
      val vecsum = math.sqrt(oriDocVec.map(x => x * x).sum)
      val docVec = oriDocVec.map(r => r / vecsum)
      (r._1, r._2.mkString(","),docVec.mkString(","))
    })

    val trDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      docVecRDD.toDF("id","words","vec")
    }
    DXPUtils.saveDataFrame(trDF, desTable, out_dt, sparkEnv.hiveContext)
  }
}
