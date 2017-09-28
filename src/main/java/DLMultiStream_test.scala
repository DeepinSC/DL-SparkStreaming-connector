import java.nio.charset.StandardCharsets.UTF_8

import DLconnector.DLUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by rick on 2017/9/28.
  */
object DLMultiStream_test {
  def main(args: Array[String]): Unit = {
    if(args.length<1){
      System.err.println("Error: 1 parameter needed!")
      System.exit(1)
    }

    val dlUriStr = args.head

    val sparkConf = new SparkConf().setAppName("DLWordCount").setMaster("local").set("spark.ui.port","7077")
    val ssc =  new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val stream = DLUtils.createDLStream(dlUriStr,ssc)
    val line = stream.flatMap(LogRecord => (new String(LogRecord.getPayload,UTF_8)).split(" ")).map(rec=> (rec,1)).reduceByKey(_+_)
    line.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
