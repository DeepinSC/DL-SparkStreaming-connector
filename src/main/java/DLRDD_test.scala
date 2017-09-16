import java.nio.charset.StandardCharsets.UTF_8


import com.myspark.DLconnector.DLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by rick on 2017/9/15.
  */
object DLRDD_test {
  val dlUriStr = "distributedlog://127.0.0.1:7000/messaging/distributedlog"
  val streamname = "basic-stream-2"
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DLWordCount").setMaster("local").set("spark.ui.port","7077")
    val sc =  new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    val rdd = DLUtils.createDLRDD(dlUriStr,streamname,sc,50,0)
    val line = rdd.map(LogRecord => (new String(LogRecord.getPayload,UTF_8),1L)).reduceByKey(_+_)
    line.foreach(x=>println(x))
    println((">>>Total records processed:"+rdd.count())+"<<<")


  }
}
