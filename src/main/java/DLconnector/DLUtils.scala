package DLconnector

import java.net.URI

import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder
import com.twitter.distributedlog.{DistributedLogConfiguration, LogRecordWithDLSN}
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
  def createDLStream(dlUriStr: String,
                     streamname:String,
                     ssc:StreamingContext,
                     maxpartperRDD:Int,
                     maxrecperpart:Int,
                     firsttxid:Long
                    ):InputDStream[LogRecordWithDLSN] = {

    new DLInputDStream(dlUriStr: String, streamname, ssc, maxpartperRDD,maxrecperpart,firsttxid)
  }
  def createDLRDD(dlUriStr: String,
                  streamname:String,
                  sc:SparkContext,
                  maxrecperpart:Int,
                  firsttxid:Long
                 ):RDD[LogRecordWithDLSN] = {

    def getPartitionList():List[Long] = {
      val uri: URI = URI.create(dlUriStr)
      val conf = new DistributedLogConfiguration().setEnableReadAhead(false)
      val namespace = DistributedLogNamespaceBuilder.newBuilder().conf(conf).uri(uri).build
      val dlm = namespace.openLog(streamname)
      val firsttxid = dlm.getFirstTxId
      val lasttxid = dlm.getLastTxId

      val res = (firsttxid to lasttxid).toList
      namespace.close()
      res

    }

    val txidList = getPartitionList()
    new DLRDD(sc,dlUriStr,streamname,txidList,maxrecperpart,firsttxid)

  }




}


