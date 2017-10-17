package DLconnector

import java.net.URI

import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder
import com.twitter.distributedlog.{DistributedLogConfiguration, LogRecordWithDLSN}
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.InputDStream
import scala.collection.JavaConversions._
/**
  * Created by rick on 2017/9/27.
  */
class DLMultiInputDStream (dlUriStr: String,ssc:StreamingContext)extends InputDStream[LogRecordWithDLSN](ssc){

  var current_streams_untilDLSN:Map[String,String] = Map()
  var current_streams_fromDLSN:Map[String,String] = Map()

  def getDLSNRange(streams:List[String]):Map[String,String] = {
    val uri: URI = URI.create(dlUriStr)
    val conf = new DistributedLogConfiguration()
    val namespace = DistributedLogNamespaceBuilder.newBuilder().conf(conf).uri(uri).build
    val temp = streams.map{
      case(streamName)=>{
        val dlm = namespace.openLog(streamName)
        val record = {
          if(dlm.getLogRecordCount==0)
            "NULL"
          else
            dlm.getLastDLSN.serialize()
        }
        val stream_tuple = (streamName,record)
        stream_tuple
      }
    }
    temp.toMap
  }
  def getStreams:List[String]={
    val uri: URI = URI.create(dlUriStr)
    val conf = new DistributedLogConfiguration()
    val namespace = DistributedLogNamespaceBuilder.newBuilder().conf(conf).uri(uri).build
    val streams = namespace.getLogs.toList
    streams
  }

  val streams = getStreams
  current_streams_fromDLSN = getDLSNRange(streams)
  var isFirsttime:Boolean = false

  override def compute(validTime: Time): Option[DLMultiRDD] = {

    current_streams_untilDLSN = getDLSNRange(streams)
    val sc = context.sparkContext
    sc.setLogLevel("Error")
    val rdd = new DLMultiRDD(sc,dlUriStr,current_streams_fromDLSN,current_streams_untilDLSN,isFirsttime)
    current_streams_fromDLSN = current_streams_untilDLSN
    isFirsttime = false
    Some(rdd)
  }


  override def start(): Unit = this.synchronized{

  }

  override def stop(): Unit = this.synchronized {

  }
}
