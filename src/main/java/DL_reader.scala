import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8

import com.twitter.distributedlog._
import com.twitter.distributedlog.namespace.{DistributedLogNamespace, DistributedLogNamespaceBuilder}
import com.twitter.distributedlog.util.FutureUtils
import com.twitter.util.FutureEventListener

/**
  * Created by rick on 2017/9/16.
  */
object DL_reader {
  def main(args: Array[String]): Unit = {
    val dlUriStr = "distributedlog://127.0.0.1:7000/messaging/distributedlog"
    val uri = URI.create(dlUriStr)
    val conf = new DistributedLogConfiguration
    val namespace = DistributedLogNamespaceBuilder.newBuilder.conf(conf).uri(uri).build
    val dlm = namespace.openLog("basic-stream-3")

    val firstDLSN = dlm.getFirstDLSNAsync.get

    val reader = dlm.getInputStream(130)
    val lasttxid = dlm.getLastTxId
    val bulk = reader.readBulk(false,(lasttxid-130).toInt)

    val readListener = new FutureEventListener[LogRecordWithDLSN]() {
      override def onFailure(cause: Throwable): Unit = { // executed when read failed.
        System.out.println("read failed\n")
        cause.printStackTrace(System.err)
        Runtime.getRuntime.exit(0)
      }

      override

      def onSuccess(record: LogRecordWithDLSN): Unit = { // process the record
        System.out.println(">" + new String(record.getPayload, UTF_8))
        System.out.println(">" + record.toString)
        //System.out.println(">"+ record.getSequenceId());
        // issue read next
        //reader.readNext.addEventListener(this)
      }
    }
    println(bulk.get(0).toString)
    //reader.readNext.addEventListener(readListener)
    //FutureUtils.result(reader.asyncClose());
  }
}
