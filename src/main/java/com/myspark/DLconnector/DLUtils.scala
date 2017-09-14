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
    new DLInputDStream(dlUriStr: String,streamname: String, ssc: StreamingContext,3,2)
  }
  def createDLRDD(dlUriStr: String,streamname:String,sc:SparkContext):RDD[LogRecordWithDLSN] = {
    val partMap = getPartitionMap(dlUriStr,streamname)
    val maxrecperpart = 50
    new DLRDD(sc,dlUriStr,streamname,partMap,maxrecperpart)
  }


  def getPartitionMap(dlUriStr: String,streamname:String):Map[Long,Int] = {
    val uri: URI = URI.create(dlUriStr)
    val conf = new DistributedLogConfiguration().setEnableReadAhead(false)
    val namespace = DistributedLogNamespaceBuilder.newBuilder().conf(conf).uri(uri).build
    val dlm = namespace.openLog(streamname)
    val firsttxid = dlm.getFirstTxId
    val recordcount = dlm.getLogRecordCount
    val reader = dlm.getInputStream(firsttxid)
    val bulk = reader.readBulk(false,recordcount.toInt)

    val res = bulk.toArray.map{case(x:LogRecordWithDLSN)=>x.getTransactionId}.zipWithIndex.toMap
    reader.close()
    namespace.close()
    res

  }

}


