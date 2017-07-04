package wordseg2

/**
  * Created by duanxiping on 2017/2/6.
  */
/* slice函数不用判断开始与结束
      scala> Array(1,2,3,4).slice(1,3)
      res13: Array[Int] = Array(2, 3)

      scala> Array(1,2,3,4).slice(-1,3)
      res14: Array[Int] = Array(1, 2, 3)

      scala> Array(1,2,3,4).slice(2,8)
      res15: Array[Int] = Array(3, 4)

      scala> Array(1,2,3,4).slice(-1,0)
      res16: Array[Int] = Array()
      */

import common.DXPUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

object TextRank {
  val srcTable = "algo.dxp_label_word_seg"
  val desTable = "algo.dxp_label_textrank_words"

  val midTable = "algo.dxp_label_textrank_mid"
  val stopWordTable = "algo.dxp_label_stopwords"
  val sparkEnv = new SparkEnv("TextRank")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val out_dt = args(1)
    val selectSQL = s"select words,id,msg from ${srcTable} where stat_date=${dt}"
    val wordsDF = sparkEnv.hiveContext.sql(selectSQL)

    val max_iter = 20
    val min_diff = 0.00001
    val window = 5
    val d = 0.85

    //对每篇文章进行TextRank计算
    val trRDD = wordsDF.repartition(200).map(l=>{
      val wordsArr = l.getAs[String](0).split(",")
      val words = new mutable.HashMap[String,Set[String]]()
      //取得每个词在指定窗口下词集合
      for(i <- 0 until wordsArr.length){
        val initVal = wordsArr.slice(i-window,i).toSet |
          wordsArr.slice(i+1,i+window).toSet
        val value = if (!words.contains(wordsArr(i)))
          initVal
        else
          initVal | words(wordsArr(i))
        words.put(wordsArr(i),value)
      }

      var score = new mutable.HashMap[String,Double]()
      var max_diff = 1.0

      // 指定迭代次数及误差阀值
      for(_ <- 0 until max_iter if max_diff >= min_diff){
        val m = new mutable.HashMap[String,Double]()
        max_diff = 0.0
        words.foreach(r=>{
          val key = r._1
          val value = r._2
          m.put(key, 1 - d)
          value.foreach(other=>{
            val size = words.get(other).size
            if( key != other && size > 0){
              val initVal = if(score.contains(other)) score(other)
                  else 0
              m.put(key,m(key)+ d/size * initVal)
            }
          })
          val newdiff = if(score.contains(key))
            math.abs(m(key) - score(key)) else math.abs(m(key))
          if(max_diff < newdiff){
           max_diff = newdiff
          }
        })
        score = m
      }
      // 保留排名靠前的80%的词
      //score.toArray.sortWith(_._2 > _._2).take((score.size*0.8).toInt)
      (score.toArray.sortWith(_._2>_._2),l.getAs[String](1),l.getAs[String](2))
    })//.flatMap(r=>r)

    trRDD.cache()

    // 保留排序后结果，以便检查TextRank排序结果
    val midRDD = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      trRDD.map(r=>{
        (r._2,r._3,r._1.mkString(","))
      }).toDF("id","msg","textrank")
    }
    DXPUtils.saveDataFrame(midRDD,midTable,out_dt,sparkEnv.hiveContext)

    // 在同一篇文章中按分数进行排序.
    // 最后对分数归一化后进行相同词合并
    val trDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      trRDD.flatMap(r=>{
        r._1
      }).reduceByKey(_ + _).toDF("word","trval")
    }
    DXPUtils.saveDataFrame(trDF,desTable,out_dt,sparkEnv.hiveContext)

    val stopWordsDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      trRDD.flatMap(r=>{
        r._1.slice((r._1.length*0.8).toInt, r._1.length).map(w=>{
          (w._1,1.0)
        })
      }).reduceByKey(_ + _).toDF("word","score")
    }
    DXPUtils.saveDataFrame(stopWordsDF.filter(stopWordsDF("score")>1000).select("word"),
      stopWordTable,
      out_dt+"02", sparkEnv.hiveContext)
  }
}
