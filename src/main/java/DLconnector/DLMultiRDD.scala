package DLconnector

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8

import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder
import com.twitter.distributedlog.{DLSN, DistributedLogConfiguration, LogRecordWithDLSN}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Created by rick on 2017/9/27.
  */
class DLMultiRDD(sc: SparkContext, dlUriStr: String, fromDLSNMap:Map[String,String],untilDLSNMap:Map[String,String],isFirstTime:Boolean) extends RDD[LogRecordWithDLSN](sc,Nil) with Logging{
  override def persist(newLevel: StorageLevel): this.type = {
    logError("DL LogRecord is not serializable. " +
      "Use .map to extract fields before calling .persist or .window")
    super.persist(newLevel)
  }
  override def compute(split: Partition, context: TaskContext): Iterator[LogRecordWithDLSN] = {
    /*Get partition info*/
    val part = split.asInstanceOf[DLMultiPartitions]
    val streamname = part.streamname
    val fromDLSN = DLSN.deserialize(part.startDLSN)
    val untilDLSN = DLSN.deserialize(part.lastDLSN)
    if ((fromDLSN == "NULL") || (fromDLSN.equals(untilDLSN)))
      Iterator.empty
    else{
      /*Open dlm*/
      val uri: URI = URI.create(dlUriStr)
      val conf = new DistributedLogConfiguration().setEnableReadAhead(false)
      val namespace = DistributedLogNamespaceBuilder.newBuilder().conf(conf).uri(uri).build
      val dlm = namespace.openLog(streamname)
      val reader = dlm.getInputStream(fromDLSN)


      val lastrec = dlm.getLastLogRecord

        reader.readNext(false)
      var result = List(reader.readNext(false))

      while(!result.last.getDlsn.equals(untilDLSN)) {
        val record = reader.readNext(false)
        result ++= List(record)
      }
      val res = result.toIterator
      reader.close()
      dlm.close()
      namespace.close()
      res
    }
  }
  override def getPartitions: Array[Partition] = {
    val rangeArray = fromDLSNMap.map{case(k,v)=>(k,(v,untilDLSNMap.apply(k)))}.toArray
    rangeArray.zipWithIndex.map{
      case(partinfo,index)=>{
        new DLMultiPartitions(index,partinfo._1,partinfo._2._1,partinfo._2._2)
      }
    }.sortBy(x => x.index).asInstanceOf[Array[Partition]]
  }
}
