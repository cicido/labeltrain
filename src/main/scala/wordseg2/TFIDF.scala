package wordseg2

/**
  * Created by duanxiping on 2017/2/6.
  */
import common.DXPUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object TFIDF {
  val srcTable = "algo.dxp_label_word_seg"
  val desTable = "algo.dxp_label_tfidf_words_more"

  val sparkEnv = new SparkEnv("TFIDF")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val selectSQL = s"select id,words from ${srcTable} where stat_date=${dt}"
    val wordsDF = sparkEnv.hiveContext.sql(selectSQL)
    val docNum = wordsDF.count()
    wordsDF.cache()

    val tfRDD = wordsDF.flatMap(r=>{
      val id = r.getAs[String](0)
      r.getAs[String](1).split(",").map(w=>{
        ((id,w),1)
      })
    }).reduceByKey(_ + _)

    val idfMap = tfRDD.map(r=>(r._1._2,1)).reduceByKey(_ + _).
      filter(_._2 > 1).map(r=>{
      (r._1,math.log(docNum+1)/(r._2+1))
    }).collectAsMap

    val tfidfRDD = tfRDD.filter(r=>{
      idfMap.contains(r._1._2)
    }).map(r=>{
      val id = r._1._1
      val w = r._1._2
      val wet = idfMap(w)*r._2*1000
      (id,Array((w,wet)))
    }).reduceByKey(_ ++ _).map(r=>{
      val idfStr = r._2.sortWith(_._2 > _._2).slice(0,r._2.length-1).map(w=>{
        w._1+";" + w._2
      }).mkString(",")
      (r._1,idfStr)
    })

    val tfidfDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      tfidfRDD.toDF("id","word_with_tfidf")
    }
    DXPUtils.saveDataFrame(tfidfDF,desTable,dt,sparkEnv.hiveContext)
  }
}
