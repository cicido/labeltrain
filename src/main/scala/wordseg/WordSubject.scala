package wordseg

/**
  * Created by duanxiping on 2017/2/6.
  */

import common.DXPUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

object WordSubject {
  val srcTable = "algo.dxp_label_subject_words"
  val desTable = "algo.dxp_label_subject_words_new"

  val sparkEnv = new SparkEnv("WordSubject")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val thres = args(1).toDouble
    val outDt = dt + (thres * 10).toLong.toString
    val selectSQL = s"select id, vec from ${srcTable} where " +
      s"stat_date=${dt} limit 10000"
    val docVecRDD = sparkEnv.hiveContext.sql(selectSQL).map(r => {
      val id = r.getAs[String](0).replaceAll(",", "|")
      val vec = r.getAs[String](1).split(",").map(r => (r.toDouble * 1000000).toInt / 1000000.0)
      (id, vec)
    })

    val simRDD = cosineSim(docVecRDD,thres)

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
    val brDocVecArr = sparkEnv.sc.broadcast(docVecArr)
    docVecRDD.flatMap(r => {
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
      }
      if(flag) Array(r)
      else{
        val mid = id + "," + res._1
        val tmpvec = (0 until r._2.length).map(x => r._2(x) + res._2(x))
        val sum = math.sqrt(tmpvec.map(x => x * x).sum)
        val mvec = tmpvec.map(w => (w*1000000 / sum).toInt / 1000000.0).toArray
        Array(r,(mid, mvec))
      }

      /*if(!flag) (0,r._1,r._2)
      else{
        i = 0
        while (flag && i < bigDocArr.length) {
          val w = bigDocArr(i)
          val sim = (0 until r._2.length).map(x => w._2(x) * r._2(x)).sum
          if (sim > thres) {
            res = w
            flag = false
          }
        }
        if (flag) (1, r._1, r._2)
        else {
          val mid = id + "," + res._1
          val tmpvec = (0 until r._2.length).map(x => r._2(x) + res._2(x))
          val sum = math.sqrt(tmpvec.map(x => x * x).sum)
          val mvec = tmpvec.map(w => (w * 1000000 / sum).toInt / 1000000.0).toArray
          (1,mid, mvec)
        }
      }
      */
    })

    val filtedArr = simRDD.map(_._1).filter(_.split(",").length>1).
      map(r=> {
        val arr = r.split(",")
        (arr(0), arr(1))
      }).reduceByKey((a,b)=>{
      if(a>b) b else a
    }).flatMap(r=>{
      Array(r._1,r._2)
    }).collect()


    simRDD.filter(r=>{
      filtedArr.contains(r._1)
    }).map(r=>{
      (r._1.replaceAll(",","|"),r._2)
    })
  }


}
