package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{Expression, Attribute, ScalaUdf, Row}
import org.apache.spark.sql.catalyst.types.IntegerType
import org.scalatest.FunSuite

import scala.collection.mutable
import scala.collection.mutable.ArraySeq
import scala.util.Random

case class Student(sid: Int, gpa: Float)
case class testObj(num1:Int, num2:Int, num3:Int)
case class testObj2(num1:Int, num2:Int, num3:Int, msg:String)

class CS186UtilsSuite extends FunSuite {
  val numberGenerator: Random = new Random()

  val studentAttributes: Seq[Attribute] =  ScalaReflection.attributesFor[Student]

  // TESTS FOR TASK #3
  /* NOTE: This test is not a guarantee that your caching iterator is completely correct.
     However, if your caching iterator is correct, then you should be passing this test. */
  test("caching iterator") {
    val list: ArraySeq[Row] = new ArraySeq[Row](1000)

    for (i <- 0 to 999) {
      list(i) = (Row(numberGenerator.nextInt(10000), numberGenerator.nextFloat()))
    }


    val udf: ScalaUdf = new ScalaUdf((sid: Int) => sid + 1, IntegerType, Seq(studentAttributes(0)))

    val result: Iterator[Row] = CachingIteratorGenerator(studentAttributes, udf, Seq(studentAttributes(1)), Seq(), studentAttributes)(list.iterator)

    assert(result.hasNext)

    result.foreach((x: Row) => {
      val inputRow: Row = Row(x.getInt(1) - 1, x.getFloat(0))
      assert(list.contains(inputRow))
    })
  }

  test("sequence with 1 UDF") {
    val udf: ScalaUdf = new ScalaUdf((i: Int) => i + 1, IntegerType, Seq(studentAttributes(0)))
    val attributes: Seq[Expression] = Seq() ++ studentAttributes ++ Seq(udf)

    assert(CS186Utils.getUdfFromExpressions(attributes) == udf)
  }

  //Tests added by Daxi Li
  val testObjAttributes: Seq[Attribute] =  ScalaReflection.attributesFor[testObj]
  val testObjAttributes2: Seq[Attribute] =  ScalaReflection.attributesFor[testObj2]
  test("caching iterator, UDP with multiple argument") {
    val list: ArraySeq[Row] = new ArraySeq[Row](1000)
    val correctResult: ArraySeq[Row] = new ArraySeq[Row](1000)
    for (i <- 0 to 999) {
      val num1: Int = numberGenerator.nextInt(10000)
      val num2: Int = numberGenerator.nextInt(10000)
      val num3: Int = numberGenerator.nextInt(10000)
      list(i) = Row(num1, num2, num3)
      correctResult(i) = Row(num1 + num2 + num3 + 1)
    }

    val udf: ScalaUdf = new ScalaUdf((num1: Int, num2: Int, num3:Int) => num1+num2+num3+1, IntegerType, Seq(testObjAttributes(0), testObjAttributes(1), testObjAttributes(2)))
    val result: Iterator[Row] = CachingIteratorGenerator(testObjAttributes, udf, Seq(), Seq(), testObjAttributes)(list.iterator)
    assert(result.hasNext)
    var total:Int = 0
    result.foreach((x: Row) => {
      assert(correctResult.contains(x))
      total+=1
    })
    assert(total==1000)
  }

  test("caching iterator, UDP with multiple argument, results more than one") {
    val list: ArraySeq[Row] = new ArraySeq[Row](1000)
    val correctResult: ArraySeq[Row] = new ArraySeq[Row](1000)
    val correctResult2: ArraySeq[Row] = new ArraySeq[Row](1000)
    for (i <- 0 to 999) {
      val num1: Int = numberGenerator.nextInt(10000)
      val num2: Int = numberGenerator.nextInt(10000)
      val num3: Int = numberGenerator.nextInt(10000)
      list(i) = Row(num1, num2, num3, "hi")
      correctResult(i) = Row("hi", num1 + num2 + num3 + 1)
      correctResult2(i) = Row("hi", num1 + num2 + num3 + 1, "hi")
    }

    val udf: ScalaUdf = new ScalaUdf((num1: Int, num2: Int, num3:Int) => num1+num2+num3+1, IntegerType, Seq(testObjAttributes2(0), testObjAttributes2(1), testObjAttributes2(2)))
    val result: Iterator[Row] = CachingIteratorGenerator(testObjAttributes2, udf, Seq(testObjAttributes2(3)), Seq(), testObjAttributes2)(list.iterator)
    val result2: Iterator[Row] = CachingIteratorGenerator(testObjAttributes2, udf, Seq(testObjAttributes2(3)), Seq(testObjAttributes2(3)), testObjAttributes2)(list.iterator)
    assert(result.hasNext)
    var total:Int = 0
    result.foreach((x: Row) => {
      assert(correctResult.contains(x))
      total+=1
    })
    assert(total==1000)
    var total1:Int = 0
    result2.foreach((x: Row) => {
      assert(correctResult2.contains(x))
      total1+=1
    })
    assert(total1==1000)
  }

/* this test is commented out because it needs to be eye ball by putting two println in cache.Contains and not contains
  test("caching behaviour"){
    val list: ArraySeq[Row] = new ArraySeq[Row](1000)

    for (i <- 0 to 999) {
      list(i) = Row(2, numberGenerator.nextFloat())
    }



    val udf: ScalaUdf = new ScalaUdf((sid: Int) => sid + 1, IntegerType, Seq(studentAttributes(0)))

    val result: Iterator[Row] = CachingIteratorGenerator(studentAttributes, udf, Seq(studentAttributes(1)), Seq(), studentAttributes)(list.iterator)
    result.foreach((x: Row) => {
      val inputRow: Row = Row(x.getInt(1) - 1, x.getFloat(0))
      assert(list.contains(inputRow))
    })
  }
*/
}