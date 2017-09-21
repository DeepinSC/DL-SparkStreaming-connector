package DLconnector

import java.nio.charset.StandardCharsets.UTF_8

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by rick on 2017/9/15.
  */
object DLRDD_test {

  def main(args: Array[String]): Unit = {
    if(args.length<5){
      System.err.println("Error: 5 parameter needed!")
      System.exit(1)
    }
    val Array(dlUriStr,streamname,maxPartitions,maxRecordPerPart,fromTxid) = args
    val sparkConf = new SparkConf().setAppName("DLWordCount").setMaster("local").set("spark.ui.port","7077")
    val sc =  new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    val rdd = DLUtils.createDLRDD(dlUriStr,streamname,sc,maxRecordPerPart.toInt,fromTxid.toInt)
    val line = rdd.map(LogRecord => (new String(LogRecord.getPayload,UTF_8),1L)).reduceByKey(_+_)
    line.foreach(x=>println(x))
    println((">>>Total records processed:"+rdd.count())+"<<<")


  }
}
