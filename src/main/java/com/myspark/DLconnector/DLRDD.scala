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
    val result = new DLIterator(part,context)
    result
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
    val index = 0
    Array(new DLPartition(index,recordrange))
  }



  private class DLIterator(part:DLPartition,context:TaskContext)extends Iterator[LogRecordWithDLSN]{
    val namespace = dlnamespace()
    val dlm = dlmanager(namespace)

    val recordnum = dlm.getLogRecordCount

    val firstTxid = dlm.getFirstTxId()


    var readcount = 0
    var currentTxid:Long = firstTxid
    //val reader: AsyncLogReader = FutureUtils.result(dlm.openAsyncLogReader(currentTxid))
    val reader:LogReader = dlm.getInputStream(currentTxid)

    context.addTaskCompletionListener{ context => closeIfNeeded() }

    def closeIfNeeded(): Unit = {
      if (!hasNext()){

        //reader.asyncClose()

        //FutureUtils.result(reader.asyncClose())
        //reader.wait(10)
        //reader.close()
        dlm.close()
        namespace.close()



      }
    }

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

    override def next(): LogRecordWithDLSN = {
      assert(hasNext(), "Can't call getNext() once last record has been reached")

      //val nextrecord = reader.readNext


      val nextrecord = reader.readNext(false)

      //nextrecord.addEventListener(readListener)
      //val record = nextrecord.get()
      val record = nextrecord
      currentTxid = record.getSequenceId
      readcount +=1
      record
    }

    override def hasNext(): Boolean =
      if (readcount<10){
        true
      }
      else{
        false
      }

  }
}
