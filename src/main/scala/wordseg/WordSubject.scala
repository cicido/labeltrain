package wordseg

/**
  * Created by duanxiping on 2017/2/6.
  */

import common.DXPUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

object WordSubject {
  val srcTable = "algo.dxp_label_subject_words"
  val desTable = "algo.dxp_label_subject_words"

  val sparkEnv = new SparkEnv("WordSubject")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val outDt = args(1)
    val thres = args(2).toDouble
    val selectSQL = s"select id, vec from ${srcTable} where " +
      s"stat_date=${dt}"
    val docVecRDD = sparkEnv.hiveContext.sql(selectSQL).map(r => {
      val id = r.getAs[String](0).replaceAll(",", "|")
      val vec = r.getAs[String](1).split(",").map(r => (r.toDouble * 1000000).toInt / 1000000.0)
      (id, vec)
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
        (r._1, r._2.mkString(","))
      }).toDF("id", "vec")
    }
    DXPUtils.saveDataFrame(trDF, desTable, outDt, sparkEnv.hiveContext)
  }

  def cosineSim(docVecRDD: RDD[(String, Array[Double])], thres:Double): RDD[(String,Array[Double])] = {
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
      while (flag && i < docArr.length){
        val w = docArr(i)
        val sim = (0 until r._2.length).map(x => w._2(x) * r._2(x)).sum
        if(sim > thres){
          res = w
          flag = false
        }
        i += 1
      }
      if(flag) Array(r)
      else {
        val mid = id + "," + res._1
        val tmpvec = (0 until r._2.length).map(x => r._2(x) + res._2(x))
        val sum = math.sqrt(tmpvec.map(x => x * x).sum)
        val mvec = tmpvec.map(w => (w * 1000000 / sum).toInt / 1000000.0).toArray
        Array(r, (mid, mvec))
      }
    })

    simRDD.cache()
    val filtedArr = simRDD.map(_._1).filter(_.split(",").length>1).
      collect().map(r=>{
      r.split(",")
    })

    val docMap = new mutable.HashMap[String,Int]()

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
      (r._1.replaceAll(",","|"),r._2)
    })
  }
}
