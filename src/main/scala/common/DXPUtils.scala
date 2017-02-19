package common

import com.hankcs.hanlp.HanLP
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import scala.collection.JavaConversions._

/**
  * Created by duanxiping on 2017/2/5.
  */
object DXPUtils {

  def saveDataFrame(df: DataFrame, outTable: String, dt: String,
                    hiveContext: HiveContext): Unit = {
    val cols = df.columns
    val sma = df.schema
    val colsType = cols.map(r => {
      sma(r).dataType match {
        case IntegerType => "int"
        case LongType => "bigint"
        case StringType => "string"
        case BooleanType => "boolean"
        case DoubleType => "double"
      }
    })

    val colsString = cols.zip(colsType).map(r => r._1 + " " + r._2).mkString(",")
    val create_table_sql: String = s"create table if not exists $outTable " +
      s" ($colsString) partitioned by (stat_date bigint) STORED AS RCFILE"
    println(create_table_sql)
    hiveContext.sql(create_table_sql)

    val tmptable = "dxp_tmp_table"
    df.registerTempTable(tmptable)

    val insert_sql: String = s"insert overwrite table $outTable partition(stat_date = $dt) " +
      s"select * from $tmptable"
    hiveContext.sql(insert_sql)
    hiveContext.dropTempTable(tmptable)
  }

  def segMsg(msg: String): Array[String] = {
    HanLP.segment(msg).map(r => {
      r.word
    }).toArray
  }

  def segMsgWithNature(msg: String): Array[(String, String)] = {
    HanLP.segment(msg).map(r => {
      (r.word, r.nature.toString)
    }).toArray
  }
}
