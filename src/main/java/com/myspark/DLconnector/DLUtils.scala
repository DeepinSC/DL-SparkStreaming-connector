package com.myspark.DLconnector


import java.net.URI

import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder
import com.twitter.distributedlog.{DistributedLogConfiguration, DistributedLogManager, LogRecord, LogRecordWithDLSN}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
;

/**
 * Created by rick on 2017/9/6.
 */
object DLUtils extends Logging{
  def createDLStream(dlUriStr: String,streamname:String,ssc:StreamingContext):InputDStream[LogRecordWithDLSN] = {
    new DLInputDStream(dlUriStr: String,streamname: String, ssc: StreamingContext, 3,2,1)
  }
  def createDLRDD(dlUriStr: String,streamname:String,sc:SparkContext):RDD[LogRecordWithDLSN] = {
    val txidList = getPartitionList(dlUriStr,streamname)
    val maxrecperpart = 50
    val firsttxid  = 0
    new DLRDD(sc,dlUriStr,streamname,txidList,maxrecperpart,firsttxid)
  }


  def getPartitionList(dlUriStr: String,streamname:String):List[Long] = {
    val uri: URI = URI.create(dlUriStr)
    val conf = new DistributedLogConfiguration().setEnableReadAhead(false)
    val namespace = DistributedLogNamespaceBuilder.newBuilder().conf(conf).uri(uri).build
    val dlm = namespace.openLog(streamname)
    val firsttxid = dlm.getFirstTxId
    val lasttxid = dlm.getLastTxId


    // for test

    val res = (firsttxid to lasttxid).toList
    namespace.close()
    res

  }

}


