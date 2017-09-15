import com.myspark.DLconnector.DLUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.nio.charset.StandardCharsets.UTF_8
/**
  * Created by rick on 2017/9/7.
  */
object Main_process {
  val dlUriStr = "distributedlog://127.0.0.1:7000/messaging/distributedlog"
  val streamname = "basic-stream-2"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DLWordCount").setMaster("local").set("spark.ui.port","7077");
    val ssc =  new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val stream = DLUtils.createDLStream(dlUriStr,streamname,ssc)
    val line = stream.map(LogRecord => (new String(LogRecord.getPayload,UTF_8),1L)).reduceByKey(_+_)
    line.print()
    ssc.start()
    ssc.awaitTermination()
  }


  def tmain(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local").set("spark.ui.port","7077");
    val sc =  new SparkContext(sparkConf)

    val maxpart = 8
    val mapp = DLUtils.getPartitionList(dlUriStr,streamname)
    mapp.filter(p=>(p%maxpart==0)).zipWithIndex.foreach(x=>println(x))
    //print(mapp.max)
  }


  def smain(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DLWordCount").setMaster("local").set("spark.ui.port","7077")
    val sc =  new SparkContext(sparkConf)

    val rdd = DLUtils.createDLRDD(dlUriStr,streamname,sc)
    val line = rdd.map(LogRecord => (new String(LogRecord.getPayload,UTF_8),1L)).reduceByKey(_+_)
    line.foreach(x=>println(x))


  }
}
