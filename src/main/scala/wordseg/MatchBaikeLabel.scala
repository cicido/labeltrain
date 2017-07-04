package wordseg

import common.DXPUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.StructType

/**
  * Created by duanxiping on 2017/2/6.
  */
object MatchBaikeLabel {
  val srcTable = "uxip.sdl_uxip_search_keyword_d"
  val dstTable = "algo.dxp_label_imei_baike_label"


  val sparkEnv = new SparkEnv("MatchBaikeLabel")

  def getWordTag(): RDD[(String, String)] = {
    val tagTable = "algo.dxp_label_baike_word_tag_layer"

    // 目前只有21号里有跑一次标签数据
    val tagDate = 20170621
    val wordTagSQL: String = s"select word,taglayer from ${tagTable} " +
      s" where taglayer !=''  and stat_date=${tagDate}"
    sparkEnv.hiveContext.sql(wordTagSQL).map(r => {
      (r.getAs[String](0), r.getAs[String](1))
    })
  }

  def main(args: Array[String]): Unit = {
    val dt: String = args(0)
    val debug: Boolean = args(1).toLowerCase == "true"
    val wordTagRdd: RDD[(String, String)] = getWordTag()
    if(debug) {
      wordTagRdd.take(5).foreach(r => println(r._1 + "\t" + r._2))
    }
    val brWordLabelMap = sparkEnv.sc.broadcast(wordTagRdd.collectAsMap())

    // 查出一天的搜索数据
    val searchSQL = s"select imei,key_word from ${srcTable} " +
      s"where stat_date=${dt}"

    //先按逗号分割，再分词，统计词的个数，再查找词对应的类别，合并所有类别及次数
    val searchRdd = sparkEnv.hiveContext.sql(searchSQL).repartition(800).map(r => {
      val imei = r.getAs[String](0)
      val key_word = r.getAs[String](1)
      // 词性mbk是自定义的词性
      val words = r.getAs[String](1).split(",").flatMap(d => {
        DXPUtils.segMsgWithNature(d).filter(w => {
          w._1.length > 1 && (
            w._2.startsWith("mbk") ||
            w._2.startsWith("n") ||
            w._2.startsWith("mz") ||
            w._2.startsWith("muc")
          )
        })
      })

      //统计词出现的次数,然后统计词出现的tag,最后统计tag的次数
      val classids = words.map(w => (w._1, 1)).groupBy(_._1).
        map(l => (l._1, l._2.map(_._2).reduce(_ + _))).toArray. /*将map类型转成Array类型,避免flatMap出错*/
        map(w => {
        try {
          ((brWordLabelMap.value)(w._1),w._2)
        }catch {
          case e:Exception => ("",0)
        }
      }).filter(_._2 != 0).groupBy(_._1).map(l => (l._1, l._2.map(_._2).reduce(_ + _))).
        toArray.sortWith(_._2 > _._2).take(5).map(_._1)
      Row(imei, key_word, words.mkString(","), classids.mkString(","))
    })

    val schema: StructType = DXPUtils.createStringSchema(Array("imei", "key_word", "seg_word", "classids"))

    val trDF: DataFrame = sparkEnv.hiveContext.createDataFrame(searchRdd, schema)
    DXPUtils.saveDataFrame(trDF, dstTable, dt, sparkEnv.hiveContext)
  }

}
