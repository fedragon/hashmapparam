package hashmapparam

import org.apache.spark.{AccumulableParam, SparkConf}
import org.apache.spark.serializer.JavaSerializer
import scala.collection.mutable.{HashMap => MutableHashMap}

/*
 * Allows a mutable HashMap[String, Int] to be used as an accumulator in Spark.
 * Whenever we try to put (k, v2) into an accumulator that already contains (k, v1), the result
 * will be a HashMap containing (k, v1 + v2).
 *
 * Would have been nice to extend GrowableAccumulableParam instead of redefining everything, but it's
 * private to the spark package.
 */
class HashMapParam extends AccumulableParam[MutableHashMap[String, Int], (String, Int)] {

  def addAccumulator(acc: MutableHashMap[String, Int], elem: (String, Int)): MutableHashMap[String, Int] = {
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
  def addInPlace(acc1: MutableHashMap[String, Int], acc2: MutableHashMap[String, Int]): MutableHashMap[String, Int] = {
    acc2.foreach(elem => addAccumulator(acc1, elem))
    acc1
  }

  /*
   * @see org.apache.spark.GrowableAccumulableParam.zero(initialValue: R): R
   */
  def zero(initialValue: MutableHashMap[String, Int]): MutableHashMap[String, Int] = {
    val ser = new JavaSerializer(new SparkConf(false)).newInstance()
    val copy = ser.deserialize[MutableHashMap[String, Int]](ser.serialize(initialValue))
    copy.clear()
    copy
  }
}
