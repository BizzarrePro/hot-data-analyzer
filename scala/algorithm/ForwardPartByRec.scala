package algorithm

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

/**
  * Created by Aiyawocao on 2017/2/2.
  */


class ForwardPartByRec private extends AbstractForward {

  override def partition(pair: RDD[(String, RecStats)]): RDD[(String, RecStats)] = {
    val partitioner = new HashPartitioner(PROCESS_NUM) {
      override def equals(other: Any): Boolean = {
        other match {
          case part: HashPartitioner => part.numPartitions == this.numPartitions
          case _ => false
        }
      }
      override def hashCode(): Int = numPartitions
      override def getPartition(key: Any): Int = {
        key match {
          case null => 0
          case _ => Math.abs(key.hashCode()) % numPartitions
        }
      }
      override def numPartitions: Int = PROCESS_NUM
    }

    def statisticFreqEst(iter: Iterator[(String, RecStats)]): Iterator[(String, RecStats)] = {
      var container = scala.collection.mutable.HashMap[String, RecStats]()
      while(iter.hasNext) {
        val currRec = iter.next
        val value: Option[RecStats] = container.get(currRec._1)
        value match {
          case None => container += currRec
          case rec: Some[RecStats] =>
            if(rec.get.currTimeSlice != currRec._2.currTimeSlice) {
              rec.get.updatePrevTimeSlice()
              rec.get.currTimeSlice = currRec._2.currTimeSlice
              Formula.applyFreqByExpoSmoothing(rec.get, appeared = true)
            }
        }
      }

      val order: Ordering[(String, RecStats)] =
        Ordering.by[(String, RecStats), Double](_._2.estimateFrequency)
      val prioQueue = collection.mutable.PriorityQueue[(String, RecStats)]()(order)
      container.foreach(entry => prioQueue.enqueue(entry))

      var hotData = new scala.collection.mutable.ArrayBuffer[(String, RecStats)]
      for {
        time <- 0 until (container.size * HOT_DATA_PERSENT * 2).toInt
        if prioQueue.nonEmpty
      } {
        hotData += prioQueue.dequeue()
      }
      hotData.iterator
    }
    val partitioned = pair.partitionBy(partitioner)
    partitioned.mapPartitions(statisticFreqEst, preservesPartitioning = true)
  }

  override def merge(rdd: RDD[(String, RecStats)]): Seq[(String, RecStats)] = {
    val sorted = rdd.sortBy(entry => entry._2.estimateFrequency, ascending = false).take((total * HOT_DATA_PERSENT).toInt)
    sorted.toSeq
  }

}
object ForwardPartByRec extends Serializable{
  private lazy val INSTANCE = new ForwardPartByRec
  def getInstance(): ForwardPartByRec = INSTANCE
}