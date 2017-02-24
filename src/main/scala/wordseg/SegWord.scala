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
    //val corpusDF = sparkEnv.hiveContext.sql(selectSQL)
    //val segDF = segWordInDF(corpusDF, "msg","words")
    val segRDD = sparkEnv.hiveContext.sql(selectSQL).repartition(200).map(r=>{
      val id = r.getAs[String](0)
      val msg = r.getAs[String](1)
      val wordsArr = DXPUtils.segMsgWithNature(msg).filter(r=>{
        r._2.startsWith("n") && r._1.length > 1
      })
      (id,msg,wordsArr)
    })
    segRDD.cache()

    // 保留词与词性,以用于后续过滤,名词中有人名,地名,机构名及其他实体.
    // 地名与人名有大部分要过滤
    val wordDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      segRDD.flatMap(r=>{
        r._3.map(w=>{
          (w._1,Set(w._2))
        })
      }).reduceByKey(_ ++ _).
        map(r=>{
          (r._1,r._2.mkString(","))
        }).
        toDF("word", "nature")
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

  /*
  def segWordInDF(df:DataFrame,col:String,newCol:String):DataFrame = {
    val stringToString = udf[String,String]{w=>
      DXPUtils.segMsgWithNature(w).filter(r=>{
        r._2.startsWith("n") && r._1.length > 1
      }).map(_._1).mkString(",")
    }
    df.repartition(200).withColumn(newCol, stringToString(df(col)))
  }
  */
}
