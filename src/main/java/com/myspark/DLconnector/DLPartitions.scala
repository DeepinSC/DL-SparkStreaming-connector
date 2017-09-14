package com.myspark.DLconnector

import com.twitter.distributedlog.DLSN
import org.apache.spark.Partition

/**
  * Created by rick on 2017/9/7.
  */

class DLPartition(
   val index:Int,
   val length:Int,
   val streamname:String,
   val starttxid:Long
                 ) extends Partition{
  //temp function
  //def count():Int = index
   def getindex(): Int ={
    index
  }
  def getstarttxid:Long = starttxid
}
