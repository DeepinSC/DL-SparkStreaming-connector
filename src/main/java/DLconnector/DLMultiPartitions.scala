package DLconnector

import org.apache.spark.Partition
/**
  * Created by rick on 2017/9/27.
  */
class DLMultiPartitions (val index:Int,val streamname:String, val starttxid:Long,val lasttxid:Long) extends Partition{//temp function
  //def count():Int = index
  def getindex: Int = index

  def getstarttxid:Long = starttxid

  def getlasttxid: Long = lasttxid

}