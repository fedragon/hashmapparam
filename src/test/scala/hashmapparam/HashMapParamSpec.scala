package hashmapparam

import org.apache.spark.Accumulable
import scala.collection.mutable.{HashMap => MutableHashMap}
import org.scalatest._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class HashMapParamSpec extends FlatSpec with Matchers with BeforeAndAfter with SharedSparkContext {

  case class Foo(x: String)

  private var accumulator: Accumulable[MutableHashMap[String, Int], (String, Int)] = _
  private var accumulator2: Accumulable[MutableHashMap[Foo, Double], (Foo, Double)] = _

  before {
    accumulator = sc.accumulable(MutableHashMap.empty[String, Int])(new HashMapParam)
    accumulator2 = sc.accumulable(MutableHashMap.empty[Foo, Double])(new HashMapParam)
  }

  "HashMapParam" should "accumulate values" in {
    accumulator.value shouldBe MutableHashMap()

    accumulator += ("a" -> 1)
    accumulator += ("b" -> 2)

    accumulator.value shouldBe MutableHashMap("a" -> 1, "b" -> 2)
  }

  it should "accumulate Numeric values with non-String keys" in {
    accumulator2.value shouldBe MutableHashMap()

    accumulator2 += (Foo("a") -> 42)
    accumulator2 += (Foo("b") -> 3.14)
    accumulator2 += (Foo("a") -> 1.98)

    accumulator2.value shouldBe MutableHashMap(Foo("a") -> 43.98, Foo("b") -> 3.14)
  }

  it should "sum values with the same key" in {
    accumulator += ("a" -> 1)
    accumulator += ("a" -> 9)

    accumulator.value shouldBe MutableHashMap("a" -> 10)
  }

  it should "work in a distributed block" in {
    val rdd = sc.parallelize(Seq("a" -> 10, "b" -> 40, "a" -> 40), 2)

    // I cannot use the instance variable here otherwise the closure will try to serialize the Spec
    val accumulator = sc.accumulable(MutableHashMap.empty[String, Int])(new HashMapParam)

    rdd.foreachPartition { it =>
      it.foreach { kv =>
      	accumulator += kv
      }
    }

    accumulator.value shouldBe MutableHashMap("a" -> 50, "b" -> 40)
  }
}
