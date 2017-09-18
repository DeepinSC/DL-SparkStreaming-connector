import com.myspark.DLconnector.DLUtils
import org.apache.spark.{SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.nio.charset.StandardCharsets.UTF_8
/**
  * Created by rick on 2017/9/7.
  */
object DLStream_test {


  def main(args: Array[String]): Unit = {
    if(args.length<5){
      System.err.println("Error: 5 parameter needed!")
      System.exit(1)
    }

    val Array(dlUriStr,streamname,maxPartitions,maxRecordPerPart,fromTxid) = args

    val sparkConf = new SparkConf().setAppName("DLWordCount").setMaster("local").set("spark.ui.port","7077")
    val ssc =  new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val stream = DLUtils.createDLStream(dlUriStr,streamname,ssc,maxPartitions.toInt,maxRecordPerPart.toInt,fromTxid.toInt)
    val line = stream.flatMap(LogRecord => (new String(LogRecord.getPayload,UTF_8)).split(" ")).map(rec=> (rec,1)).reduceByKey(_+_)
    line.print()
    ssc.start()
    ssc.awaitTermination()
  }

}




