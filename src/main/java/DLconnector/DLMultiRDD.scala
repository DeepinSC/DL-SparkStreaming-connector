package DLconnector

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8

import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder
import com.twitter.distributedlog.{DistributedLogConfiguration, LogRecordWithDLSN}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Created by rick on 2017/9/27.
  */
class DLMultiRDD(sc: SparkContext, dlUriStr: String, fromtxidMap:Map[String,Long],untiltxidMap:Map[String,Long]) extends RDD[LogRecordWithDLSN](sc,Nil) with Logging{
  override def persist(newLevel: StorageLevel): this.type = {
    logError("DL LogRecord is not serializable. " +
      "Use .map to extract fields before calling .persist or .window")
    super.persist(newLevel)
  }
  override def compute(split: Partition, context: TaskContext): Iterator[LogRecordWithDLSN] = {
    /*Get partition info*/
    val part = split.asInstanceOf[DLMultiPartitions]
    val streamname = part.streamname
    val fromtxid = part.starttxid
    val untiltxid = part.lasttxid
    //print("fromtxid"+fromtxid+",untiltxid"+untiltxid+"\n")
    if ((fromtxid == -1) || (fromtxid==untiltxid))
      Iterator.empty
    else{
    /*Open dlm*/
    val uri: URI = URI.create(dlUriStr)
    val conf = new DistributedLogConfiguration().setEnableReadAhead(false)
    val namespace = DistributedLogNamespaceBuilder.newBuilder().conf(conf).uri(uri).build
    val dlm = namespace.openLog(streamname)
    val reader = dlm.getInputStream(fromtxid)
    var result = List(reader.readNext(false))
      var count = 0
    while(result.last.getTransactionId!=untiltxid) {
      val record = reader.readNext(false)
      //println(new String(record.getPayload,UTF_8))
      result ++= List(record)
      count+=1
      println(count)
    }
    /*Get current info*/
    val res = result.toIterator
      dlm.close()
      namespace.close()
    res
    }
  }
  override def getPartitions: Array[Partition] = {
    val rangeArray = fromtxidMap.map{case(k,v)=>(k,(v,untiltxidMap.apply(k)))}.toArray
    rangeArray.zipWithIndex.map{
      case(partinfo,index)=>{
      new DLMultiPartitions(index,partinfo._1,partinfo._2._1,partinfo._2._2)
    }
    }.sortBy(x => x.index).asInstanceOf[Array[Partition]]
  }
}
