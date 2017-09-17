import com.myspark.DLconnector.DLUtils
import org.apache.spark.{SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.nio.charset.StandardCharsets.UTF_8
/**
  * Created by rick on 2017/9/7.
  */
object DLStream_test {
  val dlUriStr = "distributedlog://127.0.0.1:7000/messaging/my_namespace"
  val streamname = "basic-stream-1"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DLWordCount").setMaster("local").set("spark.ui.port","7077")
    val ssc =  new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val stream = DLUtils.createDLStream(dlUriStr,streamname,ssc,1,20,0)

    // linecount
    //val line = stream.map(LogRecord => (new String(LogRecord.getPayload,UTF_8),1L)).reduceByKey(_+_)

    //word count
    val line = stream.flatMap(LogRecord => (new String(LogRecord.getPayload,UTF_8)).split(" ")).map(rec=> (rec,1)).reduceByKey(_+_)


    line.print()
    ssc.start()
    ssc.awaitTermination()
  }

}




