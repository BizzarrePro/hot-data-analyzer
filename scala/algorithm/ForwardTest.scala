package algorithm

/**
  * Created by Aiyawocao on 2017/2/27.
  */
object ForwardTest {
  def main(args: Array[String]): Unit = {
//    val forward = ForwardFactory(ForwardFactory.PART_BY_REC)
    val forward = ForwardFactory(ForwardFactory.PART_BY_SLICE)
    forward.execute("J:\\data\\part1.blkparse")
//    forward.execute("mail-01-sample.blkparse")
  }
}
