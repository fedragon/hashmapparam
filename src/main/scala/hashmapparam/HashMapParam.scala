package hashmapparam

import org.apache.spark.{AccumulableParam, SparkConf}
import org.apache.spark.serializer.JavaSerializer
import scala.collection.mutable.{HashMap => MutableHashMap}
import Numeric.Implicits._

/*
 * Allows a mutable HashMap[A, Numeric] to be used as an accumulator in Spark.
 * Whenever we try to put (k, v2) into an accumulator that already contains (k, v1), the result
 * will be a HashMap containing (k, v1 + v2).
 *
 * Would have been nice to extend GrowableAccumulableParam instead of redefining everything, but it's
 * private to the spark package.
 */
class HashMapParam[A, B: Numeric] extends AccumulableParam[MutableHashMap[A, B], (A, B)] {

  def addAccumulator(acc: MutableHashMap[A, B], elem: (A, B)): MutableHashMap[A, B] = {
    val (k1, v1) = elem
    acc += acc.find(_._1 == k1).map {
      case (k2, v2) => k2 -> (v1 + v2)
    }.getOrElse(elem)

    acc
  }

  /*
   * This method is allowed to modify and return the first value for efficiency.
   *
   * @see org.apache.spark.GrowableAccumulableParam.addInPlace(r1: R, r2: R): R
   */
  def addInPlace(acc1: MutableHashMap[A, B], acc2: MutableHashMap[A, B]): MutableHashMap[A, B] = {
    acc2.foreach(elem => addAccumulator(acc1, elem))
    acc1
  }

  /*
   * @see org.apache.spark.GrowableAccumulableParam.zero(initialValue: R): R
   */
  def zero(initialValue: MutableHashMap[A, B]): MutableHashMap[A, B] = {
    val ser = new JavaSerializer(new SparkConf(false)).newInstance()
    val copy = ser.deserialize[MutableHashMap[A, B]](ser.serialize(initialValue))
    copy.clear()
    copy
  }
}
