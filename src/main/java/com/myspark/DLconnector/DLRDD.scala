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
    //dlm.close()
    //namespace.close()
    result
  }




  val uri: URI = URI.create(dlUriStr)
  //@transient val config = new DistributedLogConfiguration()
  @transient private var namespace:DistributedLogNamespace = null//dlnamespace
  @transient private var dlm: DistributedLogManager = null//dlmanager(namespace)

  def dlnamespace():DistributedLogNamespace = this.synchronized{
    val conf = new DistributedLogConfiguration()

    namespace = DistributedLogNamespaceBuilder.newBuilder().conf(conf).uri(uri).build
    namespace
  }

  def dlmanager(namespace:DistributedLogNamespace):DistributedLogManager = this.synchronized{
    //val uri: URI = URI.create(dlUriStr)
    dlm = namespace.openLog(streamname)
    dlm
  }
  def dlfirsttxid(dlm:DistributedLogManager) = {
    dlm.getFirstTxId
  }

  override def getPartitions: Array[Partition] = {
   // namespace = dlnamespace()
    //dlm = dlmanager(namespace)
    //val firstTxid = dlm.getFirstTxId
    //dlm.close()
    //namespace.close()
    val index = 0
    Array(new DLPartition(index,recordrange))
  }



  private class DLIterator(part:DLPartition,context:TaskContext)extends Iterator[LogRecordWithDLSN]{
    namespace = dlnamespace()
    System.out.println("-----><-----")
    dlm = dlmanager(namespace)
    val firstTxid = dlm.getFirstTxId()
    var readcount = 0
    var currentTxid:Long = firstTxid
    val reader: AsyncLogReader = FutureUtils.result(dlm.openAsyncLogReader(currentTxid))


    context.addTaskCompletionListener{ context => closeIfNeeded() }

    def closeIfNeeded(): Unit = {
      if (readcount>=recordrange){
        FutureUtils.result(reader.asyncClose())
        //FutureUtils.result(reader.asyncClose(), Duration.apply(5, TimeUnit.SECONDS))
        dlm.close()
        System.out.println("----->close<-----")
        namespace.close()


      }
    }

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
