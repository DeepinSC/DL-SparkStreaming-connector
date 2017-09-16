package com.myspark.DLconnector

import java.net.URI

import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder
import com.twitter.distributedlog._
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.InputDStream

/**
  * Created by rick on 2017/9/6.
  */
class DLInputDStream(dlUriStr: String,streamname:String,ssc:StreamingContext,maxpartperRDD:Int,maxrecperpart:Int,firsttxid:Long) extends InputDStream[LogRecordWithDLSN](ssc){




  def getPartitionList(recordcount:Long,fromtxid:Long):List[Long] = {
      val reslist = {if (recordcount!=0){
        (fromtxid to fromtxid+recordcount-1).toList
      }
      else{List.empty}
      }
      reslist
  }

  var current_fromtxid = firsttxid

  override def compute(validTime: Time): Option[DLRDD] = {

    /* need to be implemented*/

    val uri: URI = URI.create(dlUriStr)
    val conf = new DistributedLogConfiguration()
    val namespace = DistributedLogNamespaceBuilder.newBuilder().conf(conf).uri(uri).build
    val dlm = namespace.openLog(streamname)
    val recordcount = Array(maxrecperpart*maxpartperRDD,(dlm.getLastTxId-current_fromtxid+1)).min
    println(">"+recordcount)
    val txidList = getPartitionList(recordcount,current_fromtxid)
    println("txidlist:"+txidList)

    //dlm.close()
    //namespace.close()

    val sc = context.sparkContext
    sc.setLogLevel("Error")
    val rdd = new DLRDD(sc,dlUriStr,streamname,txidList,maxrecperpart,firsttxid)
    //rdd.foreach(rec=>println(">>"+new String(rec.getPayload)))
    current_fromtxid+=recordcount.toInt
    Some(rdd)
  }

  override def start(): Unit = this.synchronized{

  }

  override def stop(): Unit = this.synchronized {

  }
}
