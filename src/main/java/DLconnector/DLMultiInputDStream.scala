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

  var current_streams_untiltxid:Map[String,Long] = Map()
  var current_streams_fromtxid:Map[String,Long] = Map()

  def getTxidRange(streams:List[String],mode:String):Map[String,Long] = {
    val uri: URI = URI.create(dlUriStr)
    val conf = new DistributedLogConfiguration()
    val namespace = DistributedLogNamespaceBuilder.newBuilder().conf(conf).uri(uri).build
    val temp = streams.map{
      case(streamName)=>{
        val dlm = namespace.openLog(streamName)
        val record = {
          if(dlm.getLogRecordCount==0)
            -1
          else if(mode=="first")
            dlm.getFirstTxId
          else
            dlm.getLastTxId
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
  current_streams_fromtxid = getTxidRange(streams,"first")


  override def compute(validTime: Time): Option[DLMultiRDD] = {

    //val uri: URI = URI.create(dlUriStr)
    //val conf = new DistributedLogConfiguration()
    //val namespace = DistributedLogNamespaceBuilder.newBuilder().conf(conf).uri(uri).build
    //val streams = namespace.getLogs.toList//.asInstanceOf[Iterator[String]].toList

    current_streams_untiltxid = getTxidRange(streams,"last")

    val sc = context.sparkContext
    sc.setLogLevel("Error")
    val rdd = new DLMultiRDD(sc,dlUriStr,current_streams_fromtxid,current_streams_untiltxid)
    current_streams_fromtxid = current_streams_untiltxid
    //println("******"+current_streams_untiltxid)
    Some(rdd)
  }


  override def start(): Unit = this.synchronized{

  }

  override def stop(): Unit = this.synchronized {

  }
}
