package com.myspark.DLconnector

import java.net.URI

import com.twitter.distributedlog.namespace.{DistributedLogNamespace, DistributedLogNamespaceBuilder}
import com.twitter.distributedlog._

import org.apache.spark.internal.Logging
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.collection.JavaConversions._


/**
  * Created by rick on 2017/9/6.
  */
class DLRDD(sc: SparkContext,dlUriStr: String,streamname:String,txidList:List[Long],maxrecperpart:Int,firsttxid:Long) extends RDD[LogRecordWithDLSN](sc,Nil) with Logging{



  override def persist(newLevel: StorageLevel): this.type = {
    logError("DL LogRecord is not serializable. " +
      "Use .map to extract fields before calling .persist or .window")
    super.persist(newLevel)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[LogRecordWithDLSN] = {
    val part = split.asInstanceOf[DLPartition]
    val recordnum = part.length
    val firstTxid = part.starttxid
    println("|recnum,frtid:"+recordnum,firstTxid+"|")
    val namespace = dlnamespace(recordnum)
    val dlm = dlmanager(namespace)
    if (recordnum==0){
      Iterator.empty
    }
    else {
      val reader = dlm.getInputStream(firstTxid)
      val bulk = reader.readBulk(false, recordnum).iterator()
      reader.close()
      dlm.close()
      namespace.close()
      bulk.foreach(rec=>println(">>"+new String(rec.getPayload)))
      bulk
    }
  }


  def dlnamespace(recordnum:Int):DistributedLogNamespace = this.synchronized{
    val uri: URI = URI.create(dlUriStr)
    val conf = new DistributedLogConfiguration().setEnableReadAhead(false)
      DistributedLogNamespaceBuilder.newBuilder().conf(conf).uri(uri).build
  }

  def dlmanager(namespace:DistributedLogNamespace):DistributedLogManager = this.synchronized{
      val dlm = namespace.openLog(streamname)
      dlm
  }


  override def getPartitions: Array[Partition] = {



      if (txidList.isEmpty){
        Array(new DLPartition(0,0,streamname,-1))
      }
    else {
      val firsttxid = txidList.min
      val lastidx = txidList.max
      val zippedMap = txidList.filter(txid => ((txid - firsttxid) % maxrecperpart == 0)).zipWithIndex.map {
        case (txid, idx) =>
          if (lastidx - txid + 1 < maxrecperpart) {
            new DLPartition(idx, (lastidx - txid+1).toInt, streamname, txid)
          }
          else {
            new DLPartition(idx, maxrecperpart, streamname, txid)
          }
      }.toArray.sortBy(x => x.index).asInstanceOf[Array[Partition]]
      zippedMap
    }


   }


}
