package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, Row, Attribute}
import org.apache.spark.sql.execution.joins.dns.GeneralSymmetricHashJoin
import org.apache.spark.sql.execution.{Record, ComplicatedRecord, PhysicalRDD, SparkPlan}
import org.apache.spark.sql.test.TestSQLContext._
import org.scalatest.FunSuite

import scala.collection.immutable.HashSet
import java.util.{HashMap => JavaHashMap, ArrayList => JavaArrayList, Iterator => JavaIterator}

class SymmetricHashJoinSuite extends FunSuite {
  // initialize Spark magic stuff that we don't need to care about
  val sqlContext = new SQLContext(sparkContext)
  val recordAttributes: Seq[Attribute] = ScalaReflection.attributesFor[Record]
  val complicatedAttributes: Seq[Attribute] = ScalaReflection.attributesFor[ComplicatedRecord]

  import sqlContext.createSchemaRDD

  // initialize a SparkPlan that is a sequential scan over a small amount of data
  val smallRDD1: RDD[Record] = sparkContext.parallelize((1 to 100).map(i => Record(i)), 1)
  val smallScan1: SparkPlan = PhysicalRDD(recordAttributes, smallRDD1)
  val smallRDD2: RDD[Record] = sparkContext.parallelize((51 to 150).map(i => Record(i)), 1)
  val smallScan2: SparkPlan = PhysicalRDD(recordAttributes, smallRDD2)

  // the same as above but complicated
  val complicatedRDD1: RDD[ComplicatedRecord] = sparkContext.parallelize((1 to 100).map(i => ComplicatedRecord(i, i.toString, i*2)), 1)
  val complicatedScan1: SparkPlan = PhysicalRDD(complicatedAttributes, complicatedRDD1)
  val complicatedRDD2: RDD[ComplicatedRecord] = sparkContext.parallelize((51 to 150).map(i => ComplicatedRecord(i, i.toString, i*2)), 1)
  val complicatedScan2: SparkPlan = PhysicalRDD(complicatedAttributes, complicatedRDD2)



 test ("simple join") {
    val outputRDD = GeneralSymmetricHashJoin(recordAttributes, recordAttributes, smallScan1, smallScan2).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()

    outputRDD.collect().foreach(x => seenValues = seenValues + x)

    (51 to 100).foreach(x => assert(seenValues.contains(new JoinedRow(Row(x), Row(x)))))
  }

  test ("complicated join") {
    val outputRDD = GeneralSymmetricHashJoin(Seq(complicatedAttributes(0)), Seq(complicatedAttributes(0)), complicatedScan1, complicatedScan2).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()

    outputRDD.collect().foreach(x => seenValues = seenValues + x)

    (51 to 100).foreach(x => assert(seenValues.contains(new JoinedRow(Row(x, x.toString, x*2), Row(x, x.toString, x*2)))))
  }


  //added by Daxi Li
  val noMatchRDD1: RDD[ComplicatedRecord] = sparkContext.parallelize((1 to 100).map(i => ComplicatedRecord(i, i.toString, i*2)), 1)
  val noMatchScan1: SparkPlan = PhysicalRDD(complicatedAttributes, noMatchRDD1)
  val noMatchRDD2: RDD[ComplicatedRecord] = sparkContext.parallelize((101 to 150).map(i => ComplicatedRecord(i, i.toString, i*2)), 1)
  val noMatchScan2: SparkPlan = PhysicalRDD(complicatedAttributes, noMatchRDD2)
  test ("no matching join                     [Added by Daxi]"){
    val outputRDD = GeneralSymmetricHashJoin(Seq(complicatedAttributes(0)), Seq(complicatedAttributes(0)), noMatchScan1, noMatchScan2).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()
    outputRDD.collect().foreach(x => seenValues = seenValues + x)
    assert(seenValues.size == 0)
  }

  val someRDD1: RDD[ComplicatedRecord] = sparkContext.parallelize((1 to 100).map(i => ComplicatedRecord(i, i.toString, i*2)), 1)
  val someScan1: SparkPlan = PhysicalRDD(complicatedAttributes, someRDD1)
  val emptyRDD2: RDD[ComplicatedRecord] = sparkContext.parallelize((0 until 0).map(i => ComplicatedRecord(i, i.toString, i*2)), 1)
  val emptyScan2: SparkPlan = PhysicalRDD(complicatedAttributes, emptyRDD2)
  test ("empty join                           [Added by Daxi]"){
    val outputRDD = GeneralSymmetricHashJoin(Seq(complicatedAttributes(0)), Seq(complicatedAttributes(0)), someScan1, emptyScan2).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()
    outputRDD.collect().foreach(x => seenValues = seenValues + x)
    assert(seenValues.size == 0)
  }

  val singleRDD1: RDD[ComplicatedRecord] = sparkContext.parallelize((2 to 2).map(i => ComplicatedRecord(i, i.toString, i*2)), 1)
  val singleScan1: SparkPlan = PhysicalRDD(complicatedAttributes, singleRDD1)
  val singleRDD2: RDD[ComplicatedRecord] = sparkContext.parallelize((2 to 2).map(i => ComplicatedRecord(i, i.toString, i*2)), 1)
  val singleScan2: SparkPlan = PhysicalRDD(complicatedAttributes, singleRDD2)

  test ("single join                          [Added by Daxi]"){
    val outputRDD = GeneralSymmetricHashJoin(Seq(complicatedAttributes(0)), Seq(complicatedAttributes(0)), singleScan1, singleScan2).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()
    outputRDD.collect().foreach(x => seenValues = seenValues + x)
    assert(seenValues.size == 1)
    assert(seenValues.contains(new JoinedRow(Row(2,"2", 4), Row(2,"2",4))))
  }

  val largeRDD1: RDD[ComplicatedRecord] = sparkContext.parallelize((2 to 20000).map(i => ComplicatedRecord(i, i.toString, i*2)), 1)
  val largeScan1: SparkPlan = PhysicalRDD(complicatedAttributes, largeRDD1)
  val largeRDD2: RDD[ComplicatedRecord] = sparkContext.parallelize((100 to 40000).map(i => ComplicatedRecord(i, i.toString, i*2)), 1)
  val largeScan2: SparkPlan = PhysicalRDD(complicatedAttributes, largeRDD2)

  test ("large join                           [Added by Daxi]"){
    val outputRDD = GeneralSymmetricHashJoin(Seq(complicatedAttributes(0)), Seq(complicatedAttributes(0)), largeScan1, largeScan2).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()
    outputRDD.collect().foreach(x => seenValues = seenValues + x)
    (100 to 20000).foreach(x => assert(seenValues.contains(new JoinedRow(Row(x, x.toString, x*2), Row(x, x.toString, x*2)))))
  }

  val manyRDD1: RDD[ComplicatedRecord] = sparkContext.parallelize((1 to 200).map(i => ComplicatedRecord(20,"20", 20*2)), 1)
  val manyScan1: SparkPlan = PhysicalRDD(complicatedAttributes, manyRDD1)
  val manyRDD2: RDD[ComplicatedRecord] = sparkContext.parallelize((1 to 1).map(i => ComplicatedRecord(20, "20", 20*2)), 1)
  val manyScan2: SparkPlan = PhysicalRDD(complicatedAttributes, manyRDD2)
  test("many matching: one small one big     [Added by Daxi]"){
    val outputRDD = GeneralSymmetricHashJoin(Seq(complicatedAttributes(0)), Seq(complicatedAttributes(0)), manyScan1, manyScan2).execute()
    val seenValues: JavaArrayList[Row] = new JavaArrayList[Row]()
    outputRDD.collect().foreach(x => seenValues.add(x))
    val size:Int = seenValues.size()
    assert(size == 200)
    (0 to 199).foreach(x => assert(seenValues.get(x)==(new JoinedRow(Row(20, "20", 20*2), Row(20, "20", 20*2)))))
  }
}

































