package wordseg

import common.DXPUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by duanxiping on 2017/2/6.
  */
object SegWord {
  val srcTable = "algo.dxp_label_corpus"
  val dstTable = "algo.dxp_label_word_seg"
  val sparkEnv = new SparkEnv("SegWord")
  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val out_dt = args(1)
    val selectSQL = s"select id,msg from ${srcTable}"
    val corpusDF = sparkEnv.hiveContext.sql(selectSQL)
    val segDF = segWordInDF(corpusDF, "msg","words")
    DXPUtils.saveDataFrame(segDF,dstTable,out_dt,sparkEnv.hiveContext)
  }

  def segWordInDF(df:DataFrame,col:String,newCol:String):DataFrame = {
    val stringToString = udf[String,String]{w=>
      DXPUtils.segMsgWithNature(w).filter(r=>{
        r._2.startsWith("n") && r._1.length > 1
      }).map(_._1).mkString(",")
    }
    df.repartition(200).withColumn(newCol, stringToString(df(col)))
  }
}
