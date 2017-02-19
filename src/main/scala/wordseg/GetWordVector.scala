package wordseg

import common.DXPUtils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
//import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.SparkContext
import org.apache.spark.mllib.logword2vec.Word2Vec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
/**
  * Created by duanxiping on 2017/1/19.
  */
object GetWordVector {
  val srcTable = "algo.dxp_label_word_seg"
  val dstTable = "algo.dxp_label_word_vec"
  val sparkEnv = new SparkEnv("Word2Vec")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val minCount = args(1).toInt
    val selectSQL = s"select words from $srcTable"
    val segDF = sparkEnv.hiveContext.sql(selectSQL)

    val segRDD = segDF.map(_.getAs[String](0).split(",").toSeq)

    val word2vec = new Word2Vec().
      setVectorSize(200).
      setMinCount(minCount).
      setNumPartitions(100)

    val model = word2vec.fit(segRDD)

    val wordVecDF: DataFrame = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      val wordVec = model.getVectors.mapValues(vec => Vectors.dense(vec.map(_.toDouble)))
      sc.parallelize(wordVec.toSeq).toDF("word", "vector")
    }

    val vecToString = udf[String,Vector]{ w=>{
      w.toArray.mkString(",")
    }}

    val wordResDF = wordVecDF.
      withColumn("vecString",vecToString(wordVecDF("vector"))).
      select("word","vecString")
    wordResDF.printSchema()

    DXPUtils.saveDataFrame(wordResDF, dstTable,dt,sparkEnv.hiveContext)


    /*
    val word2Vec = new Word2Vec().
      setInputCol("words").
      setOutputCol("result").
      setVectorSize(200).
      setMinCount(1)

    val model = word2Vec.fit(segDF)
    val result = model.transform(segDF)
    val vecToString = udf[String,Vector]{ w=>{
      w.toArray.mkString(",")
    }}
    val wordVecDF =  model.getVectors
    val wordResDF = wordVecDF.
      withColumn("vecString",vecToString(wordVecDF("vector"))).
      select("word","vecString")
    wordResDF.printSchema()

    saveDataFrame(wordResDF, dstTable,dt)
    */
  }



}
