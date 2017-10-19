package algorithm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

/**
  * Created by Aiyawocao on 2017/2/27.
  */
abstract class AbstractForward extends Serializable {
  var INTERVAL: Long = 0
  val ALPHA: Double = 0.05
  val HOT_DATA_PERSENT: Double = 0.5
  var PROCESS_NUM = 0
  val SLICE_NUM = 1000
  @transient
  val conf = new SparkConf().setAppName("app").setMaster("local").set("spark.driver.memory", "2g")
  @transient
  val sc = new SparkContext(conf)
  var total = 1L

  def partition(pairs: RDD[(String, RecStats)]): RDD[(String, RecStats)]
  def merge(rdd: RDD[(String, RecStats)]): Seq[(String, RecStats)]
  //TODO need to modify partition num
  def readFile(path: String): RDD[(String, RecStats)] = {
    val records = sc.textFile(path)
    total = records.count()
    PROCESS_NUM = records.partitions.length
    INTERVAL = total / SLICE_NUM
    val recsNumInPart = total / PROCESS_NUM
    val pairs = records.mapPartitionsWithIndex((index, iter) => {
      var recordsSeq = new scala.collection.mutable.ArrayBuffer[(String, RecStats)]
      var recordNum = index * recsNumInPart
      println("in partition: " + recordNum + " " + index)
      while(iter.hasNext) {
        val arr = iter.next.split(" ")
        if(arr.length == 9) {
          val timeSlice = (recordNum / INTERVAL + 1).toInt
          recordsSeq += (arr(3) -> new RecStats(timeSlice, arr(3)))
        }
        recordNum += 1
      }
      recordsSeq.iterator
    }, preservesPartitioning = true)
    pairs
  }
  object Formula extends Serializable {
    //TODO debug, 'appeared' is used to decay the record which does't appear in last time slice
    def applyFreqByExpoSmoothing(recStat: RecStats,
                                 appeared: Boolean) = {
      val exp = recStat.currTimeSlice - recStat.prevTimeSlice
      val decayFactor = Math.pow(1 - ALPHA, exp)
      if(appeared)
        recStat.estimateFrequency = ALPHA + recStat.estimateFrequency * decayFactor
      else  recStat.estimateFrequency = recStat.estimateFrequency * decayFactor
    }

    def applyPartEstimateAccumulation(recStats: RecStats,
                                      timeSlice: Int) = {
      val decayFactor = Math.pow(1 - ALPHA, timeSlice)
      recStats.estimateFrequency = decayFactor + recStats.estimateFrequency
    }

    def obtainOverallFreq(recStats: RecStats,
                          timeSlice: Int,
                          prevEst: Double) = {
      val decayFactor = Math.pow(1 - ALPHA, timeSlice) * prevEst
      recStats.estimateFrequency = decayFactor + ALPHA * recStats.estimateFrequency
    }
  }
  case class RecStats(blockAddress: String,
                      var currTimeSlice: Int,
                      var prevTimeSlice: Int,
                      var estimateFrequency: Double) {
    def this(timeSlice: Int, blockAddr: String) = this(blockAddr, timeSlice, timeSlice, 0.0)

    def updatePrevTimeSlice() = prevTimeSlice = currTimeSlice

    override def toString: String =
      "Address: " + blockAddress + " EstimatedFrequency: " + estimateFrequency
  }

  def execute(path: String): Unit = {
    val time = System.currentTimeMillis()
    val pairs = readFile(path)
    val part = partition(pairs)
    val heat = merge(part)
    //heat.foreach(tup => println(tup._2))
    println(System.currentTimeMillis() - time)
  }
}
