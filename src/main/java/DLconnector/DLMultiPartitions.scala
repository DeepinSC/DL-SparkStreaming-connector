package DLconnector

import org.apache.spark.Partition
/**
  * Created by rick on 2017/9/27.
  */
class DLMultiPartitions (val index:Int,val streamname:String, val startDLSN:String,val lastDLSN:String) extends Partition{//temp function
//def count():Int = index
def getindex: Int = index

  def getstartDLSN:String = startDLSN

  def getlastDLSN: String = lastDLSN

}