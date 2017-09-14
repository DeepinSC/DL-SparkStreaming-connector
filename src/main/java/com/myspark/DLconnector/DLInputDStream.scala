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
class DLInputDStream(dlUriStr: String,streamname:String,ssc:StreamingContext,maxrecperpart:Int) extends InputDStream[LogRecordWithDLSN](ssc){



  def getPartitionMap(dlUriStr: String,streamname:String):Map[Long,Int] = {
    val uri: URI = URI.create(dlUriStr)
    val conf = new DistributedLogConfiguration()
    val namespace = DistributedLogNamespaceBuilder.newBuilder().conf(conf).uri(uri).build
    val dlm = namespace.openLog(streamname)
    val firsttxid = dlm.getFirstTxId
    val recordcount = dlm.getLogRecordCount
    val reader = dlm.getInputStream(firsttxid)
    val bulk = reader.readBulk(false,recordcount.toInt)

    val res = bulk.toArray.map{case(x:LogRecordWithDLSN)=>x.getTransactionId}.zipWithIndex.toMap
    namespace.close()
    reader.close()
    res

  }

  override def compute(validTime: Time): Option[DLRDD] = {

    /* need to be implemented*/
    val partMap = getPartitionMap(dlUriStr,streamname)
    val rdd = new DLRDD(context.sparkContext,dlUriStr,streamname,partMap,maxrecperpart)
    Some(rdd)
  }

  override def start(): Unit = this.synchronized{

  }

  override def stop(): Unit = this.synchronized {

  }
}
