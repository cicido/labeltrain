package wordseg

import common.DXPUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by duanxiping on 2017/2/6.
  */
object TopLabel {
  val srcTable = "algo.dxp_label_imei_label"
  val dstTable = "algo.dxp_label_top_label"
  val sparkEnv = new SparkEnv("TopLabel")

  def main(args: Array[String]): Unit = {
    val dt = args(0)

    val classSQL = s"select classnames from ${srcTable} " +
      s"where stat_date=${dt}"
    val classDF = sparkEnv.hiveContext.sql(classSQL).flatMap(r=>{
      r.getAs[String](0).split(",").map(c=>(c,1))
    }).reduceByKey(_ + _)

    val trDF = {
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      classDF.toDF("classid","imei_count")
    }
    DXPUtils.saveDataFrame(trDF, dstTable, dt, sparkEnv.hiveContext)
  }
}
