package wordseg

/**
  * Created by duanxiping on 2017/2/6.
  */
import common.DXPUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object MyHashingTFTest {
  val srcTable = "algo.dxp_label_word_seg"
  val desTable = "algo.dxp_label_tfidf_words"

  val stopWordsTable = "algo.dxp_label_stopwords"
  val sparkEnv = new SparkEnv("TFIDF")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val selectSQL = s"select words from ${srcTable} where stat_date=${dt}"
    val wordsDF = sparkEnv.hiveContext.sql(selectSQL)
    val stringToSeq = udf[Seq[String],String]{ w => {
      w.split(",").toSeq
    }}
    val splitsDF = wordsDF.withColumn("word",stringToSeq(wordsDF("words")))
    val midRDD:RDD[Array[(String,Int)]] = splitsDF.select("word").map(r=>{
      r.getAs[Seq[String]](0).map(r=>(r,1)).groupBy(_._1).
        map(l => (l._1, l._2.map(_._2).reduce(_+_))).toArray
    }).cache()

    val docNum = midRDD.count()

    val tfMap:Map[String,Int] = midRDD.flatMap(r=>r).reduceByKey(_ + _).collect().toMap
    val idfMap:Map[String,Int] = midRDD.map(r=>{
      r.map(w=>(w._1,1))
    }).flatMap(r=>r).reduceByKey(_ + _).collect().toMap

    val tfidfDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._

      val tfidfArr = tfMap.map(r=>{
        (r._1,r._2,idfMap.getOrElse(r._1,0),
          r._2 * math.log((docNum + 1.0) / (idfMap.getOrElse(r._1,0) + 1.0)))
      })
      sc.parallelize(tfidfArr.toSeq).toDF("word", "tf","df","tfidf")
    }
    DXPUtils.saveDataFrame(tfidfDF,desTable,dt,sparkEnv.hiveContext)

    // 过滤条件设置
    val stopWordsDF = tfidfDF.filter(tfidfDF("tf")> docNum/10
      or (tfidfDF("tf")< tfidfDF("df")*1.2 and tfidfDF("df") > docNum/20)
      or tfidfDF("df") > docNum/10).select("word")

    DXPUtils.saveDataFrameWithType(stopWordsDF,stopWordsTable,dt+"_idf",
      sparkEnv.hiveContext, "string")

    /*
    val hashingTF = new MLHashingTF().
      setInputCol("word").
      setOutputCol("rawFeatures")

    val featurizedData = hashingTF.transform(splitsDF)
    val hashMap = hashingTF.hashMap
    val invHashMap = hashMap.toSeq.map(r=>(r._2,r._1)).toMap

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    val tfidfArr = rescaledData.select("features").map()

    val vecToString = udf[String,Vector]{w=>{
      w match {
        case SparseVector(size, indices, values) =>
          indices.map(r=>invHashMap(r)).zip(values).
            map(r=>r._1+"|"+r._2).mkString(",")
        case DenseVector(values) =>
          values.zip(0 until values.length).filter(_._1 > 0).
            map(r=>invHashMap(r._2)+"|" + r._1).mkString(",")
        case other =>
          throw new UnsupportedOperationException(
            s"Only sparse and dense vectors are supported but got ${other.getClass}.")
      }
    }}

    val resData = featurizedData.withColumn("vecString",vecToString(featurizedData("rawFeatures"))).select("words","vecString")
    DXPUtils.saveDataFrame(resData,desTable,dt,sparkEnv.hiveContext)
    */
  }
}
