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
class DLInputDStream(dlUriStr: String,streamname:String,ssc:StreamingContext,recordrange:Int) extends InputDStream[LogRecordWithDLSN](ssc){


  override def compute(validTime: Time): Option[DLRDD] = {

    /* need to be implemented*/

    val rdd = new DLRDD(context.sparkContext,dlUriStr,streamname,recordrange)
    Some(rdd)
  }

  override def start(): Unit = this.synchronized{

  }

  override def stop(): Unit = this.synchronized {
  }
}
