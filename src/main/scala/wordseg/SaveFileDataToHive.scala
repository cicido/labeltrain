package wordseg

/**
  * Created by duanxiping on 2017/1/18.
  */
import common.DXPUtils
import org.apache.spark.sql.Row

object SaveFileDataToHive {
  val sparkEnv = new SparkEnv("SaveRuleWords")

  def main(args: Array[String]): Unit = {
    if(args.length != 5){
      println(s"Usage: main parameter <path> <sep> <dstTable> <fields> <stat_date>")
      sys.exit(1)
    }
    val path = args(0)
    val sep = args(1)
    val dstTable = args(2)
    val fields = args(3)
    val dt = args(4)
    /*
    val st = fields.split(",").
      map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(st)
     */
    val schema = DXPUtils.createStringSchema(fields.split(","))
    val fieldLen = schema.length
    if(fieldLen < 1 ||fieldLen > 6){
      println(s"not supported fieldLen: ${fieldLen}")
      sys.exit(1)
    }

    println("*"*20 )
    println(s"path:${path}")
    println(s"sep:a${sep}a")
    println(s"dstTable:${dstTable}")
    println(s"fields:${fields}")
    println(s"dt:${dt}")
    println(s"fieldLen ${fieldLen}")

    val wordTagRdd = sparkEnv.sc.wholeTextFiles(path).flatMap(r => {
        r._2.split("\n")
      }).filter(_.split(sep).length == fieldLen).map(r=>{
        val arr = r.split(sep)
        Row.fromSeq(arr)
      })
    val trDF = sparkEnv.hiveContext.createDataFrame(wordTagRdd,schema)
    DXPUtils.saveDataFrame(trDF, dstTable, dt, sparkEnv.hiveContext)
  }
}
