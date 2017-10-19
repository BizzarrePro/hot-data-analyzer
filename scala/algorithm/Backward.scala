package algorithm

import java.io.FileNotFoundException
import java.util.logging.{Level, Logger}
import scala.collection.mutable.ArrayBuffer

import akka.actor._

import scala.annotation.tailrec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}

import scala.collection.mutable
/**
  * Created by Aiyawocao on 2017/2/11.
  */
object Constant {
  val INTERVAL: Int = 2624
//  val INTERVAL: Int = 100
  val ALPHA: Double = 0.05
  val HOT_DATA_PERSENT: Double = 0.01
//  val HOT_DATA_PERSENT: Double = 0.1
  //partition number
  val PART_NUM = 4
  var END_SLICE = 0L
  val poisonPill: PartStats = PartStats(-1, 0, 0)
}

case class RecStats(blockAddr: Long,
                    var timeSlice: Long,
                    var backEst: Double,
                    var lowerEstBound: Double,
                    var upperEstBound: Double) {

  def this(timeSlice: Long, blockAddr: Long) = this(blockAddr, timeSlice, 0.0, 0.0, 0.0)

  def updateBoundingAccFreqEst(currTimeSlice: Long, endTimeSlice: Long): Unit =
    backEst = backEst + Constant.ALPHA * Math.pow(1 - Constant.ALPHA, endTimeSlice - currTimeSlice)

  def updateLowerEst(startTimeSlice: Long, endTimeSlice: Long): Unit =
    lowerEstBound = backEst + Math.pow(1 - Constant.ALPHA, endTimeSlice - startTimeSlice + 1)

  def updateUpperEst(currTimeSlice: Long, endTimeSlice: Long): Unit =
    upperEstBound = backEst + Math.pow(1 - Constant.ALPHA, endTimeSlice - currTimeSlice + 1)

}

case class SubscribeReceiver(receiverActor: ActorRef)

case class PartStats(knth: Double,
                     lowCount: Int,
                     upCount: Int) {
  def this(lowCount: Int, upCount: Int) = this(0.0, lowCount, upCount)
  def this() = this(0, 0)
}

object Backward {
  //use enumuation
  val logger = Logger.getGlobal()
  logger.setLevel(Level.INFO)
  abstract class Command {}
  class Initialize extends Command {}
  class ReportCounts(val threshold: Double) extends Command {}
  class TightenBounds extends Command {}
  class Finalize(val threshhold: Double) extends Command {}
  class IllegalCommand extends Command {}
  private object CommandFactory extends Enumeration{
    type OrderType = Value
    val INIT, TIGHTEN, REPORT, FINALIZE = Value
    def apply(order: OrderType): Command =
      order match {
        case INIT     => new Initialize
        case TIGHTEN  => new TightenBounds
        case _        => new IllegalCommand
      }
    def apply(order: OrderType, threshold: Double): Command =
      order match {
        case REPORT   => new ReportCounts(threshold)
        case FINALIZE => new Finalize(threshold)
        case _        => new IllegalCommand
      }
  }
//  object PartStats {
//    def unapply(arg: PartStats): Option[(Double, Int, Int)] =
//      Some(arg.knth, arg.lowCount, arg.upCount)
//    def apply(knth: Double, lowCount: Int, upCount: Int): PartStats =
//      new PartStats(knth, lowCount, upCount)
//  }

  class Controller(hotDataSize: Long, conf: Configuration) extends Actor {
    var registry: Seq[ActorRef] = Nil
    var totalLower, totalUpper: Int = 0
    var candidate: Vector[Double] = Vector()
    var result: Seq[RecStats] = Nil
    var median, lBorder, rBorder: Int = 0
    var selectedThresh: Double = 0.0
    //TODO debug
    /* if a partition is read ahead of time,
       the subsequent command will no longer be valid for it.
    */
    override def receive: Receive = {
      case PartStats(0.0, lower, upper) =>
        handleCountReports(lower, upper)
      case initRes: PartStats =>
        handleInitialResults(initRes)
      case hotSeq: Seq[RecStats] =>
        integratePartialData(hotSeq)
      case receiver: SubscribeReceiver =>
        handleNodeHeartbeat(receiver)
    }

    def register(ref: ActorRef): Unit = registry = registry :+ ref
    def notifyAll(com: Command): Unit = registry.foreach(ref => ref ! com)

    var acceptedCount = 0
    var lValue, rValue: Double = 0.0

    def handleNodeHeartbeat(receiver: SubscribeReceiver): Unit = {
      register(receiver.receiverActor)
      logger.info(registry.size + " actor has registered")
      if(registry.size == Constant.PART_NUM)
        notifyAll(CommandFactory(CommandFactory.INIT))
    }

    def handleCountReports(lower: Int, upper: Int): Unit = {
      totalLower += lower
      totalUpper += upper
      acceptedCount += 1
      if(acceptedCount == registry.size) {
        //TODO here condition may be wrong
        //TODO replace op {&&} to {||}
        if(Math.abs(totalUpper - totalLower) == 0 || totalLower == hotDataSize)
          notifyAll(CommandFactory(CommandFactory.FINALIZE, selectedThresh))
        else {
          notifyAll(CommandFactory(CommandFactory.TIGHTEN))
          //TODO modify the condition to less than 1 replace equals 1 
          selectedThresh = if(rBorder - lBorder <= 1) {
            val average: Double = (lValue + rValue) / 2
            //TODO fix type match problem. 
            if(totalLower < hotDataSize) rValue = average
            else lValue = average
            average
          } else {
            median = ( rBorder + lBorder ) / 2
            if(totalLower < hotDataSize) {
              rBorder = median
              rValue = candidate(rBorder)
            } else {
              lBorder = median
              lValue = candidate(lBorder)
            }
            candidate(median)
          }
          notifyAll(CommandFactory(CommandFactory.REPORT, selectedThresh))
        }
        totalLower = 0
        totalUpper = 0
        acceptedCount = 0
      }
    }

    def handleInitialResults(state: PartStats): Unit = {
      totalLower += state.lowCount
      totalUpper += state.upCount
      candidate = candidate :+ state.knth
      if(candidate.size == registry.size) {
        candidate.sortBy(x => x)
        rBorder = candidate.size - 1
        median = ( rBorder + lBorder ) / 2
        selectedThresh = candidate(median)
        if(totalLower < hotDataSize)  rBorder = median
        else  lBorder = median
        totalLower = 0
        totalUpper = 0
        notifyAll(CommandFactory(CommandFactory.REPORT, selectedThresh))
      }
    }

    def integratePartialData(part: Seq[RecStats]): Unit = {
      result ++= part
      acceptedCount += 1
      if(acceptedCount == candidate.size) {
        //output position
        val sorted = result.sortBy(rec => rec.backEst)
        //sorted.foreach(rec => println("BlockAddress: " + rec.blockAddr + " EstimatedFrequency: " + rec.backEst))
        context.system.shutdown()
      }
    }
  }

  trait Executable {
    def initialize(): PartStats
    def report(threshold: Double): PartStats
    def tighten(): Unit
    def shutdown(threshold: Double): Seq[RecStats]
  }

  /**
    * @param hotDataSize  data size in each partition
    * @param filePath tell actor where is file partition
    */
  class Worker(master: ActorRef,
               hotDataSize: Long,
               filePath: String,
               conf: Configuration)
    extends Actor with Executable {
//    lazy private val master = context.actorSelection(urlOfMaster)
    val logPartition = mutable.Stack[(Long, RecStats)]()
    override def preStart(): Unit = {
      val fs = FileSystem.get(conf)
      var in: Option[FSDataInputStream] = None
      try {
        //TODO debug the relationshif between path and conf
        in = Some(fs.open(new Path(filePath)))
        var buffer = new StringBuilder
        var ch: Int = 0
        //TODO replace record format in partition file to Tuple{blockAddress, timeSlice}
        while(ch != -1) {
          if(ch.toChar == '\n') {
            val rec = buffer.toString().split(" ")
            if(rec.length == 10)
              logPartition.push((rec(3).toLong, new RecStats(rec(9).toLong, rec(3).toLong)))
            buffer = new StringBuilder()
          } else
            buffer.append(ch.toChar)
          ch = in.get.read()
        }
      } finally {
        in.get.close()
        fs.close()
      }
      master ! SubscribeReceiver(context.self)
    }

    override def receive: Receive = {
      case init: Initialize       => master ! initialize
      case rep: ReportCounts      => master ! report(rep.threshold)
      case tigh: TightenBounds    => tighten()
      case fin: Finalize          => master ! shutdown(fin.threshhold)
      case _                      => println("Unrecognized command!")
    }

    var vessel = scala.collection.mutable.HashMap[Long, RecStats]()
    var acceptedThresh = 0.0
    var overlappedValue = Double.MaxValue

    override def initialize(): PartStats = {
      var kthLower = Double.MaxValue
      @tailrec
      def fillWithCalBounds(countDown: Long): Long = {
        val element = logPartition.pop._2
        if(countDown == 0 || logPartition.isEmpty)  element.timeSlice
        else if(vessel contains element.blockAddr) fillWithCalBounds(countDown)
        else {
          element.updateBoundingAccFreqEst(element.timeSlice, Constant.END_SLICE)
          element.updateLowerEst(1, Constant.END_SLICE)
          element.updateUpperEst(element.timeSlice, Constant.END_SLICE)
          if(element.lowerEstBound < kthLower)
            kthLower = element.lowerEstBound
          vessel.put(element.blockAddr, element)
          fillWithCalBounds(countDown - 1)
        }
      }
      var prevSlice = fillWithCalBounds(hotDataSize)
      acceptedThresh = Constant.END_SLICE - Math.log(kthLower) / Math.log(1 - Constant.ALPHA)
      while(logPartition.nonEmpty) {
        val rec = logPartition.pop._2
        vessel contains rec.blockAddr match {
          case false =>
            if(rec.timeSlice > acceptedThresh) {
              rec.updateBoundingAccFreqEst(rec.timeSlice, Constant.END_SLICE)
              rec.updateLowerEst(1, Constant.END_SLICE)
              rec.updateUpperEst(rec.timeSlice, Constant.END_SLICE)
              //removed kthLower variable assignment
              vessel.put(rec.blockAddr, rec)
            }
          case true =>
            if(vessel(rec.blockAddr).timeSlice != prevSlice) {
              vessel(rec.blockAddr).timeSlice = rec.timeSlice
              vessel(rec.blockAddr) updateBoundingAccFreqEst(rec.timeSlice, Constant.END_SLICE)
            }
        }

        if(prevSlice != rec.timeSlice) {
          val order: Ordering[RecStats] = Ordering.by[RecStats, Double](_.lowerEstBound)
          val heap = scala.collection.mutable.PriorityQueue[RecStats]()(order)
          for (entry <- vessel.values) {
            entry.updateLowerEst(1, Constant.END_SLICE)
            entry.updateUpperEst(rec.timeSlice, Constant.END_SLICE)
            heap.enqueue(entry)
          }
          for(index <- 0 until hotDataSize.toInt
            if heap.nonEmpty
          ) kthLower = heap.dequeue().lowerEstBound

          heap.clear()

          for (entry <- vessel.values)
            if (entry.upperEstBound <= kthLower)
              vessel.remove(entry.blockAddr)

//          println("Stage: Initialize ThreadsInfo: " + Thread.currentThread().getName +
//            " CurrentTimeSlice: " + rec.timeSlice + " Size: " + vessel.size)
          //TODO replace to original condition
          if(Math.abs(vessel.size - hotDataSize) < 10)
            return statisticPartStatus(kthLower, isReport = false)

          acceptedThresh = Constant.END_SLICE - Math.log(kthLower) / Math.log(1 - Constant.ALPHA)
        }
        prevSlice = rec.timeSlice
      }
      //TODO solve poison pill problem
      Constant.poisonPill
    }

    override def report(threshold: Double): PartStats = {

      for {entry <- vessel.values
        minOverValue = entry.upperEstBound - threshold
      }
        if(minOverValue > 0 && minOverValue < overlappedValue)
          overlappedValue = minOverValue

      statisticPartStatus(threshold, isReport = true)
    }
    override def tighten(): Unit = {
      var kthLower = Double.MaxValue
      if(logPartition.isEmpty)
        return
      var prevSlice = logPartition.top._2.timeSlice
      val timeSliceBounds = Constant.END_SLICE + 1 - Math.log(overlappedValue) / Math.log(1 - Constant.ALPHA)
      while(logPartition.nonEmpty) {
        val rec = logPartition.pop._2
        vessel contains rec.blockAddr match {
          case false =>
            if(rec.timeSlice > acceptedThresh) {
              rec.updateBoundingAccFreqEst(rec.timeSlice, Constant.END_SLICE)
              rec.updateLowerEst(1, Constant.END_SLICE)
              rec.updateUpperEst(rec.timeSlice, Constant.END_SLICE)
              vessel.put(rec.blockAddr, rec)
            }
          case true =>
            if(vessel(rec.blockAddr).timeSlice != prevSlice) {
              vessel(rec.blockAddr).timeSlice = rec.timeSlice
              vessel(rec.blockAddr) updateBoundingAccFreqEst(rec.timeSlice, Constant.END_SLICE)
            }
        }

        if(prevSlice != rec.timeSlice) {
          println("Stage: TightenBounds ThreadsInfo: " + Thread.currentThread() + " CurrentTimeSlice: " + rec.timeSlice)
          kthLower = Double.MaxValue
          val order: Ordering[RecStats] = Ordering.by[RecStats, Double](_.lowerEstBound)
          val heap = scala.collection.mutable.PriorityQueue[RecStats]()(order)
          for (entry <- vessel.values) {
            entry.updateLowerEst(1, Constant.END_SLICE)
            entry.updateUpperEst(rec.timeSlice, Constant.END_SLICE)
            heap.enqueue(entry)
          }
          for(index <- 0 until hotDataSize.toInt
              if heap.nonEmpty
          ) kthLower = heap.dequeue().lowerEstBound

          heap.clear()

          for (entry <- vessel.values)
            if(entry.upperEstBound <= kthLower)
              vessel.remove(entry.blockAddr)


          if(rec.timeSlice <= timeSliceBounds) return

          acceptedThresh = Constant.END_SLICE - Math.log(kthLower) / Math.log(1 - Constant.ALPHA)
        }
        prevSlice = rec.timeSlice
      }
    }
    override def shutdown(threshold: Double): Seq[RecStats] = {
      val hotSeq = ArrayBuffer[RecStats]()
      for(entry <- vessel.values
        if entry.upperEstBound > threshold
      ) hotSeq += entry
      hotSeq
    }

    def statisticPartStatus(kthLower: Double, isReport: Boolean): PartStats = {
      var lowCount, upCount = 0
      for(entry <- vessel.values) {
        if(entry.lowerEstBound > kthLower)  lowCount += 1
        if(entry.upperEstBound > kthLower)  upCount += 1
      }
      isReport match {
        case false  => PartStats(kthLower, lowCount, upCount)
        case true   => PartStats(0.0, lowCount, upCount)
      }
    }

  }

  def execute(): Unit = {
//    val port = 9000
//    val ip = "0.0.0.0"
//    val uri = s"hdfs://$ip:$port/"
    val time = System.currentTimeMillis()
    var recordNum  = 1L
    var endSlice = 0L
    val uri = ""
    val tempDirName: String = "part"
    val conf = new Configuration()
    //conf.set("fs.default.name", uri)
    conf.set("hadoop.tmp.dir", "D:\\hd_data\\tmp")
    conf.set("dfs.name.dir", "D:\\hd_data\\nn")
    conf.set("dfs.data.dir", "D:\\hd_data\\dn")
    conf.set("dfs.permissions.supergroup", "hadoop")
    def partitionByHash(): Array[(String, Long)] = {
      val states: Array[(String, Long)] = new Array(Constant.PART_NUM)
      val pathList: Array[String] = new Array(Constant.PART_NUM)
      val lineCount: Array[Long] = new Array(Constant.PART_NUM)
      val fs = FileSystem.get(conf)
      val filePath = new Path("J:\\data\\part1.blkparse")

//      val filePath = new Path(s"${uri}mail-02-sample.blkparse")
      val dirPath = new Path(s"$uri$tempDirName")

      if(fs.exists(dirPath))
        fs.delete(dirPath, true)
      else
        fs.mkdirs(dirPath)

      if(!fs.exists(filePath))
        throw new FileNotFoundException()

      var in: Option[FSDataInputStream] = None
      val out: Array[FSDataOutputStream] = new Array(Constant.PART_NUM)
      for ( index <- 0 until Constant.PART_NUM) {
        pathList(index) = s"$dirPath/partition$index.blkparse"
        out(index) = fs.create(new Path(pathList(index)))
      }
      try {
        in = Some(fs.open(filePath))
        var buffer = new StringBuilder
        var ch = in.get.read()
        var timeSlice = 1L
        while(ch != -1) {
          if(ch.toChar == '\n') {
            val rec = buffer.toString
            val arr = rec.split(" ")
            if(arr.length == 9) {
              recordNum += 1
              if (recordNum % Constant.INTERVAL == 0) timeSlice += 1
              val hashIndex = Math.abs(arr(3).hashCode % Constant.PART_NUM)
              out(hashIndex).write(s"$rec $timeSlice\n".getBytes)
              lineCount(hashIndex) += 1
            }
            buffer = new StringBuilder()
          }
          else
            buffer.append(ch.toChar)
          ch = in.get.read()
        }
      } finally {
        fs.close()
        for(o <- out) o.close()
      }
      for(index <- 0 until Constant.PART_NUM)
        states(index) = (pathList(index), lineCount(index))

      endSlice = recordNum / Constant.INTERVAL + 1
      Constant.END_SLICE = endSlice

      states
    }
    val partitionStates = partitionByHash()

    val system = ActorSystem("ParallelBackwardExecutor")
    val workers: Array[ActorRef] = new Array(Constant.PART_NUM)

    val controller: ActorRef = system.actorOf(
      Props(classOf[Controller],
        (recordNum * Constant.HOT_DATA_PERSENT).toLong, conf),
      "Controller")
    println("totalRecordNum: "  + recordNum)
    for(index <- 0 until Constant.PART_NUM)
      println("partitionRecNum: " + partitionStates(index)._2 + " partitionHotDataNum: " + (Constant.HOT_DATA_PERSENT * partitionStates(index)._2).toLong)
    for(index <- 0 until Constant.PART_NUM)
      workers(index) = system.actorOf(
        Props(classOf[Worker],
              controller,
          (partitionStates(index)._2 * Constant.HOT_DATA_PERSENT).toLong,
              partitionStates(index)._1,
              conf),
        s"worker$index")

    system.awaitTermination()
    Runtime.getRuntime.addShutdownHook(new Thread(){
      override def run(): Unit = {
        val fileSystem = FileSystem.get(conf)
        val dir = new Path(s"$uri$tempDirName")
        if(fileSystem.exists(dir))
          fileSystem.delete(dir, true)
        fileSystem.close()
      }
    })
    println(System.currentTimeMillis() - time)
  }
}