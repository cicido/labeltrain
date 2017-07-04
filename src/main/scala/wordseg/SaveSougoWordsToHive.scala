package wordseg

/**
  * Created by duanxiping on 2017/1/18.
  */
import common.DXPUtils
import org.apache.spark.sql.SQLContext


object SaveSougoWordsToHive {
  val dstTable = "algo.dxp_label_sougowords"
  val sparkEnv = new SparkEnv("SaveSougoWords")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val path = args(1)
    println(path)

    val wordsDF = {
      val sqlContext = SQLContext.getOrCreate(sparkEnv.sc)
      import sqlContext.implicits._
      sparkEnv.sc.wholeTextFiles(path).repartition(20).flatMap(r => {
        println(r._2)
        r._2.split("\n")
      }).filter(_.split(",").length == 9).map(r=>{
        val arr = r.split(",")
        (arr(0),arr(1).toInt,arr(2).toInt,arr(3).toInt,arr(4).toInt, arr(5),arr(6),arr(7),arr(8))
      }).toDF("word","f_id","s_id","t_id","dict_id","f_name","s_name","t_name","dict_name")
    }
    DXPUtils.saveDataFrame(wordsDF, dstTable, dt, sparkEnv.hiveContext)
  }
}
