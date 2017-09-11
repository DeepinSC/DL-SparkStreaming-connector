import com.myspark.DLconnector.DLUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by rick on 2017/9/7.
  */
object Main_process {
  def test_main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DLWordCount").setMaster("local").set("spark.ui.port","7077");
    val ssc =  new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val dlUriStr = "distributedlog://127.0.0.1:7000/messaging/distributedlog"
    val streamname = "basic-stream-1"
    val stream = DLUtils.createDLStream(dlUriStr,streamname,ssc)
    val line = stream.map(logrecord=>logrecord.getPayload())
    val wordCounts = line.map(x => (x, 1L))
      .reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local").set("spark.ui.port","7077");
    val sc =  new SparkContext(sparkConf)
    val dlUriStr = "distributedlog://127.0.0.1:7000/messaging/distributedlog"
    val streamname = "basic-stream-1"
    val stream = DLUtils.createDLRDD(dlUriStr,streamname,sc)
    //stream.persist()
    val misery = stream.map(LogRecord => LogRecord.getPayload)
    val reco = misery.map(byterec => byterec)
    reco.foreach(rec=>println(rec))
    //val line = stream.foreach(x => println(x))
    //val wordCounts = line.map(x => (x, 1L))
    //  .reduceByKey(_ + _)
    //println(wordCounts)
  }
}