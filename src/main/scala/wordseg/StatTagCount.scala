package wordseg

import common.DXPUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by duanxiping on 2017/2/6.
  */
object StatTagCount {
  val srcTable = "algo.dxp_label_imei_baike_label"
  val dstTable = "algo.dxp_label_tag_count"

  val sparkEnv = new SparkEnv("StatTagCount")

  def main(args: Array[String]): Unit = {
    val dt: String = args(0)
    val debug: Boolean = args(1).toLowerCase == "true"

    // 查出一天的搜索数据
    val searchSQL = s"select classids from ${srcTable} " +
      s"where stat_date=${dt}"

    val searchRdd = sparkEnv.hiveContext.sql(searchSQL).repartition(200).flatMap(r => {
      r.getAs[String](0).split(",").flatMap(w=> {
        val classids = w.split("-")
        if (classids.length > 2) {
          Array(classids(0), classids.slice(0, 2).mkString("-"),
            classids.slice(0, 3).mkString("-"))
        } else if (classids.length == 2) {
          Array(classids(0), classids.mkString("-"))
        } else {
          Array(classids(0))
        }
      })
    }).map(r=>(r,1)).reduceByKey(_+_).map(r=>Row(r._1,r._2))

    val schema: StructType = DXPUtils.createSchema(Array(("classids","string"),("cnt","int")))
    val trDF: DataFrame = sparkEnv.hiveContext.createDataFrame(searchRdd, schema)
    DXPUtils.saveDataFrame(trDF, dstTable, dt, sparkEnv.hiveContext)
  }

}
