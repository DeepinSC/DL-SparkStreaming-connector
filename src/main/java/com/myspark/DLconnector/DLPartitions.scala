package com.myspark.DLconnector

import com.twitter.distributedlog.DLSN
import org.apache.spark.Partition

/**
  * Created by rick on 2017/9/7.
  */
private  class DLPartition(val index:Int,val firstTxid:Long ,val recordrange:Int) extends Partition{
  //temp function
  def count():Int = index
}
