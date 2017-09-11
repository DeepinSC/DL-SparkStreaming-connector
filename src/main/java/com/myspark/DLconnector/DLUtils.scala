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
    new DLInputDStream(dlUriStr: String,streamname: String, ssc: StreamingContext,3)
  }
  def createDLRDD(dlUriStr: String,streamname:String,sc:SparkContext):RDD[LogRecordWithDLSN] = {
    val recordrange = 3
    val uri: URI = URI.create(dlUriStr)
    val config = new DistributedLogConfiguration()
    val namespace = DistributedLogNamespaceBuilder.newBuilder().conf(config).uri(uri).build
    val dlm: DistributedLogManager = namespace.openLog(streamname)
    new DLRDD(sc,dlUriStr,streamname,recordrange)
  }

}


