package com.myspark.DLconnector

import java.net.URI

import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder
import com.twitter.distributedlog._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.InputDStream

/**
  * Created by rick on 2017/9/6.
  */
class DLInputDStream(dlUriStr: String,streamname:String,ssc:StreamingContext,maxrecperpart:Int,maxpartperRDD:Int,firsttxid:Long) extends InputDStream[LogRecordWithDLSN](ssc){




  def getPartitionList(dlm: DistributedLogManager,recordcount:Long,fromtxid:Long):List[Long] = {

    val reader = dlm.getInputStream(fromtxid)
    //val bulk = reader.readBulk(false,recordcount.toInt)
    //val res = bulk.toArray.map{case(x:LogRecordWithDLSN)=>x.getTransactionId}.zipWithIndex.toMap
    val res = (fromtxid to fromtxid+recordcount-1).toList
    //namespace.close()
    //reader.close()
    res

  }

  var current_fromtxid = firsttxid

  override def compute(validTime: Time): Option[DLRDD] = {

    /* need to be implemented*/


    val uri: URI = URI.create(dlUriStr)
    val conf = new DistributedLogConfiguration()
    val namespace = DistributedLogNamespaceBuilder.newBuilder().conf(conf).uri(uri).build
    val dlm = namespace.openLog(streamname)
    val lasttxid = dlm.getLastTxId
    val recordcount = Array(maxrecperpart*maxpartperRDD,lasttxid-current_fromtxid+1).min


    val txidList = getPartitionList(dlm,recordcount,current_fromtxid)
    val rdd = new DLRDD(context.sparkContext,dlUriStr,streamname,txidList,maxrecperpart,firsttxid)
    current_fromtxid+=recordcount.toInt
    Some(rdd)
  }

  override def start(): Unit = this.synchronized{

  }

  override def stop(): Unit = this.synchronized {

  }
}
