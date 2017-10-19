package algorithm

import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

/**
  * Created by Aiyawocao on 2017/2/5.
  */
class ForwardPartByTime private extends AbstractForward {
  var tSNumInPart = 0
  override def partition(pairs: RDD[(String, RecStats)]): RDD[(String, RecStats)] = {
    //compute average time slice number per partition, then use exponential smooth unrollment
    tSNumInPart = SLICE_NUM / PROCESS_NUM
    def statisticRecFreq(iter: Iterator[(String, RecStats)]): Iterator[(String, RecStats)] = {
      var factor = tSNumInPart
      var prevSlice = 0;
      var map = scala.collection.mutable.HashMap[String, RecStats]()
      while(iter.hasNext) {
        val currRec = iter.next
        val value: Option[RecStats] = map.get(currRec._1)
        val timeSlice = currRec._2.currTimeSlice
        value match {
          case None  =>
            map += currRec
          case rec: Some[RecStats]  =>
            //TODO Check it, it's different from implement of Java
            if(rec.get.currTimeSlice != timeSlice) {
              rec.get.currTimeSlice = timeSlice
              Formula.applyPartEstimateAccumulation(rec.get, factor)
            }
        }
        if(prevSlice != timeSlice && factor >= 0)
          factor -= 1
        prevSlice = timeSlice

      }
      map.iterator
    }
    pairs.mapPartitions(statisticRecFreq)
  }

  override def merge(rdd: RDD[(String, RecStats)]): Seq[(String, RecStats)] = {
    var globalTable = scala.collection.mutable.HashMap[String, RecStats]()
    val mapSet = rdd.glom().map(arr => arr.toMap).collect()

    def smoothFreq(mapSet: Array[Map[String, RecStats]], index: Int): Map[String, RecStats] = {
      if(index < 0)
        return null
      //println("index: " + index)
      val prev = smoothFreq(mapSet, index - 1)
      val iter = mapSet(index).iterator
      while(iter.hasNext) {
        val entry = iter.next._2
        if(prev == null)
          Formula.obtainOverallFreq(entry, (index + 1) * tSNumInPart - 1, 0)
        else {
          val prevEntry = globalTable.get(entry.blockAddress)
          Formula.obtainOverallFreq(entry, (index + 1) * tSNumInPart - 1,
            prevEntry match {
              case None => 0.0
              case rec: Some[RecStats] => rec.get.estimateFrequency
            })
        }
        globalTable += (entry.blockAddress -> entry)
      }
      mapSet(index)
    }
    smoothFreq(mapSet, mapSet.length - 1)
    val unsorted = sc.parallelize(globalTable.toSeq)
    val sorted = unsorted.sortBy(entry => entry._2.estimateFrequency, false).take((total * HOT_DATA_PERSENT).toInt)
    sorted.toSeq
  }
}
object ForwardPartByTime extends Serializable {
  private lazy val INSTANCE = new ForwardPartByTime
  def getInstance(): ForwardPartByTime = INSTANCE
}

