package algorithm

/**
  * Created by Aiyawocao on 2017/2/28.
  */
object ForwardFactory extends Enumeration{
  type Forward = Value
  val PART_BY_REC, PART_BY_SLICE = Value
  def apply(forward: Forward): AbstractForward = {
    forward match {
      case PART_BY_REC => ForwardPartByRec.getInstance()
      case PART_BY_SLICE => ForwardPartByTime.getInstance()
    }
  }
}
