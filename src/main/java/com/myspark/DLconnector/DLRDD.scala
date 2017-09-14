package com.myspark.DLconnector

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit

import com.twitter.distributedlog.namespace.{DistributedLogNamespace, DistributedLogNamespaceBuilder}
import com.twitter.distributedlog._
import com.twitter.distributedlog.util.FutureUtils
import com.twitter.util.{Duration, FutureEventListener}
import org.apache.spark.internal.Logging
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.collection.JavaConversions._


/**
  * Created by rick on 2017/9/6.
  */
class DLRDD(sc: SparkContext,dlUriStr: String,streamname:String,partMap:Map[Long,Int],maxrecperpart:Int) extends RDD[LogRecordWithDLSN](sc,Nil) with Logging{




  override def persist(newLevel: StorageLevel): this.type = {
    logError("DL LogRecord is not serializable. " +
      "Use .map to extract fields before calling .persist or .window")
    super.persist(newLevel)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[LogRecordWithDLSN] = {
    val part = split.asInstanceOf[DLPartition]
   // val result = new DLIterator(part,context)
    
    val namespace = dlnamespace()
    val dlm = dlmanager(namespace)
    val recordnum = part.length
    val firstTxid = part.starttxid
    val reader= dlm.getInputStream(firstTxid)
    val bulk = reader.readBulk(false,recordnum)
    val result  = bulk.iterator()
    reader.close()
    dlm.close()
    namespace.close()

    def closeIfNeeded(): Unit = {
      if (result.length==recordnum){
        reader.close()
        dlm.close()
        namespace.close()
      }
    }

    //context.addTaskCompletionListener{ context => closeIfNeeded() }
    bulk.iterator()
  }




  val uri: URI = URI.create(dlUriStr)

  def dlnamespace():DistributedLogNamespace = this.synchronized{
    val conf = new DistributedLogConfiguration()
      DistributedLogNamespaceBuilder.newBuilder().conf(conf).uri(uri).build
  }

  def dlmanager(namespace:DistributedLogNamespace):DistributedLogManager = this.synchronized{
      namespace.openLog(streamname)
  }

  def dlfirsttxid(dlm:DistributedLogManager) = {
    dlm.getFirstTxId
  }

  override def getPartitions: Array[Partition] = {
    //val index = 0
    //Array(new DLPartition(index,maxrecperpart))
    val lastidx = partMap.max._2
    partMap.filter(pair=>(pair._2%maxrecperpart==0)).zipWithIndex.map{
      case((txid,index),idx)=>
        if (lastidx-index<maxrecperpart){new DLPartition(idx,lastidx-index+1,streamname,txid)}
        else{new DLPartition(idx,maxrecperpart,streamname,txid)}
    }.toArray
    //Array(new DLPartition(0,0,streamname,maxrecperpart))
  }


}
