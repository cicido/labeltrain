package wordseg2

/**
  * Created by duanxiping on 2017/2/6.
  */

import common.DXPUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

object WordSubjectWithBLAS {
  val srcTable = "algo.dxp_label_docvec_with_blas"
  val desTable = "algo.dxp_label_subject_words_with_blas"

  val sparkEnv = new SparkEnv("WordSubject")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val outDt = args(1)
    val isFirst = args(2).toInt
    val thres = args(3).toDouble
    val tbName = if(isFirst == 1) srcTable else desTable
    val selectSQL = s"select id, vec,norm from ${tbName} where " +
      s"stat_date=${dt}"
    val docVecRDD = sparkEnv.hiveContext.sql(selectSQL).map(r => {
      val id = r.getAs[String](0).replaceAll(",", "|")
      val vec = Vectors.dense(r.getAs[String](1).split(",").map(_.toDouble))
      val norm = r.getAs[Double](2)
      (id, vec, norm)
    })
    docVecRDD.cache()

    var simRDD = cosineSim(docVecRDD,thres)
    //println("simRDD count:"+ simRDD.count())

    /*
    for(i <- 0 until 3){
      simRDD = cosineSim(simRDD,thres)
    }
    */


    val trDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      simRDD.map(r => {
        (r._1, r._2.toArray.mkString(","),r._3)
      }).toDF("id", "vec", "norm")
    }
    DXPUtils.saveDataFrame(trDF, desTable, outDt, sparkEnv.hiveContext)
  }


  def cosineSim(docVecRDD: RDD[(String,Vector,Double)], thres:Double):
  RDD[(String,Vector,Double)] = {
    val docVecArr = docVecRDD.collect().sortWith(_._1 < _._1)
    println("*\n" * 50)
    println(docVecArr.length)
    //docVecArr.foreach(r=>println(r._1))
    val brDocVecArr = sparkEnv.sc.broadcast(docVecArr)
    val simRDD  = docVecRDD.repartition(320).flatMap(r => {
      val id = r._1
      val docArr = brDocVecArr.value.filter(_._1 > id)
      var flag = true
      var res = r
      var i = 0
      var maxsim = 0.0
      while (i < docArr.length){
        val w = docArr(i)
        val sim = MYBLAS.dot(r._2,w._2) / (r._3 * w._3)
        if(sim > thres){
          if(sim > maxsim){
            res = w
            maxsim = sim
          }
          flag = false
        }
        i += 1
      }
      if(flag) Array(r)
      else {
        val mid = id + "," + res._1
        val mvec = Vectors.zeros(Config.vectorSize)
        MYBLAS.copy(r._2,mvec)
        MYBLAS.axpy(1.0,res._2,mvec)
        val norm = math.sqrt(MYBLAS.dot(mvec,mvec))
        Array(r, (mid, mvec,norm))
      }
    })

    simRDD.cache()
    val filtedArr = simRDD.map(_._1).filter(_.split(",").length>1).
      collect().map(r=>{
      r.split(",")
    })

    val docMap = new scala.collection.mutable.HashMap[String,Int]()

    val fArr = filtedArr.filter(r=>{
      if(docMap.contains(r(0)) || docMap.contains(r(1))){
        false
      } else{
        docMap(r(0)) = 1
        docMap(r(1)) = 1
        true
      }
    })

    val mergeArr = fArr.map(r=>{
      r.mkString(",")
    })

    val deleteSingDocArr = fArr.flatten

    simRDD.filter(r=>{
      mergeArr.contains(r._1) ||
        (r._1.split(",").length == 1 && (!deleteSingDocArr.contains(r._1)))
    }).map(r=>{
      (r._1.replaceAll(",","|"),r._2,r._3)
    })
  }
}
