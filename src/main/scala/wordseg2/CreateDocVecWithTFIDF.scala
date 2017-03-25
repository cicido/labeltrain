package wordseg2

/**
  * Created by duanxiping on 2017/2/6.
  */

import common.DXPUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext

object CreateDocVecWithTFIDF {
  val srcTable = "algo.dxp_label_tfidf_words_more"
  val desTable = "algo.dxp_label_docvec_with_blas"

  val sparkEnv = new SparkEnv("CreateDocVec")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val out_dt = args(1)
    val selectSQL = s"select id, word_with_tfidf from ${srcTable} where stat_date=${dt}"
    val docDF = sparkEnv.hiveContext.sql(selectSQL)

    val wordsSQL = s"select word,vecstring from algo.dxp_label_word_vec " +
      s"where stat_date=${dt}"

    val word2vecMap = sparkEnv.hiveContext.sql(wordsSQL).map(r => {
      val word = r.getAs[String](0)
      val vec = Vectors.dense(r.getAs[String](1).split(",").map(_.toDouble))
      (word, vec)
    }).collect().toMap

    println("*\n" * 30)
    println(word2vecMap.size)
    //word2vecMap.foreach(r=>println(r._1))

    val brVecMap = sparkEnv.sc.broadcast(word2vecMap)

    val docVecRDD = docDF.repartition(200).map(r => {
      val id = r.getAs[String](0)
      val words = r.getAs[String](1).split(",").map(w => {
        val arr = w.split(";")
        if(arr.length < 2)
          ("",0.0)
        else
          (arr(0), arr(1).toDouble)
      })
      (id,words)
    }).filter(_._2.length > 3).map(r=>{
      val vecMap = brVecMap.value
      val docVec = Vectors.zeros(Config.vectorSize)
      r._2.filter(w=>vecMap.contains(w._1)).
        map(w=>(vecMap(w._1),w._2)).foreach(v =>{
        MYBLAS.axpy(v._2,v._1,docVec)
      })
      //不需要归一化,但可以先算出向量的模
      val vecsum = math.sqrt(MYBLAS.dot(docVec,docVec))
      (r._1, r._2.map(x=>x._1+";"+x._2).mkString(","),docVec.toArray.mkString(","),vecsum)
    })

    val trDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      docVecRDD.toDF("id","words","vec","norm")
    }
    DXPUtils.saveDataFrame(trDF, desTable, out_dt, sparkEnv.hiveContext)
  }
}
