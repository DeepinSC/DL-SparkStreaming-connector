package com.myspark.DLconnector

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8

import com.twitter.distributedlog.namespace.{DistributedLogNamespace, DistributedLogNamespaceBuilder}
import com.twitter.distributedlog._
import com.twitter.distributedlog.util.FutureUtils
import com.twitter.util.FutureEventListener
import org.apache.spark.internal.Logging
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Created by rick on 2017/9/6.
  */
class DLRDD(sc: SparkContext,dlUriStr: String,streamname:String,val recordrange:Int) extends RDD[LogRecordWithDLSN](sc,Nil) with Logging{




  override def persist(newLevel: StorageLevel): this.type = {
    logError("DL LogRecord is not serializable. " +
      "Use .map to extract fields before calling .persist or .window")
    super.persist(newLevel)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[LogRecordWithDLSN] = {

    val part = split.asInstanceOf[DLPartition]
    new DLIterator(part,context)
  }



  //LogRecordWithDLSN record = dlm.getLastLogRecord();
  //DLSN lastDLSN = record.getDlsn();
  //final AsyncLogReader reader = FutureUtils.result(dlm.openAsyncLogReader(lastDLSN));


  override def getPartitions: Array[Partition] = {

    val uri: URI = URI.create(dlUriStr)
    val config = new DistributedLogConfiguration()
    val namespace = DistributedLogNamespaceBuilder.newBuilder().conf(config).uri(uri).build
    val dlm: DistributedLogManager = namespace.openLog(streamname)
    val firstTxid = dlm.getFirstTxId
    dlm.close()

    val index = 0
    Array(new DLPartition(index,firstTxid,recordrange))
  }



  private class DLIterator(part:DLPartition,context:TaskContext)extends Iterator[LogRecordWithDLSN]{

    val uri: URI = URI.create(dlUriStr)
    val config = new DistributedLogConfiguration()
    val namespace = DistributedLogNamespaceBuilder.newBuilder().conf(config).uri(uri).build
    val dlm: DistributedLogManager = namespace.openLog(streamname)
    val firstTxid = dlm.getFirstTxId()
    var readcount = 0

    var currentTxid:Long = firstTxid
    val reader: AsyncLogReader = FutureUtils.result(dlm.openAsyncLogReader(currentTxid))

    override def next(): LogRecordWithDLSN = {

      val readListener = new FutureEventListener[LogRecordWithDLSN]() {

        override def onFailure(cause: Throwable): Unit = { // executed when read failed.
          System.out.println("read failed\n")
          cause.printStackTrace(System.err)
          Runtime.getRuntime.exit(0)
        }

        override def onSuccess(record: LogRecordWithDLSN): Unit = { // process the record
          //System.out.println(">" + new String(record.getPayload, UTF_8))
          //System.out.println(">" + record.toString)
          // issue read next
          //reader.readNext.addEventListener(this)

        }

      }
      val nextrecord = reader.readNext
      nextrecord.addEventListener(readListener)
      val record = nextrecord.get()
      currentTxid = record.getSequenceId
      readcount = readcount+1
      record
    }

    override def hasNext: Boolean =
      if (readcount<recordrange){
        true
      }
    else{
        false
      }

  }
}
