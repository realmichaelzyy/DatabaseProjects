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
  val orderMatchRDD1: RDD[ComplicatedRecord] = sparkContext.parallelize((100 to 300).map(i => ComplicatedRecord(i*2, "Left", i)), 1)
  val orderMatchScan1: SparkPlan = PhysicalRDD(complicatedAttributes, orderMatchRDD1)
  val orderMatchRDD2: RDD[ComplicatedRecord] = sparkContext.parallelize((200 to 350).map(i => ComplicatedRecord(i, "Right", i*2)), 1)
  val orderMatchScan2: SparkPlan = PhysicalRDD(complicatedAttributes, orderMatchRDD2)
  test ("left right joined order check        [Added by Daxi]"){
    val outputRDD = GeneralSymmetricHashJoin(Seq(complicatedAttributes(2)), Seq(complicatedAttributes(0)), orderMatchScan1, orderMatchScan2).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()

    outputRDD.collect().foreach(x => seenValues = seenValues + x)
    assert(seenValues.size == 101)
    (200 to 300).foreach(x => assert(seenValues.contains(new JoinedRow(Row(x*2, "Left", x), Row(x, "Right", x*2)))))
  }

  def duplicateMapping(i:Int):ComplicatedRecord={
    var result:ComplicatedRecord = null
    if (i%10==0){
      result = ComplicatedRecord(999999, "I am duplicate", 999999)
    }else{
      result = ComplicatedRecord(i, "I am unique", i)
    }
    result

  }
  val dupMatchRDD1: RDD[ComplicatedRecord] = sparkContext.parallelize((1 to 350).map(duplicateMapping), 1)
  val dupMatchScan1: SparkPlan = PhysicalRDD(complicatedAttributes, dupMatchRDD1)
  val dupMatchRDD2: RDD[ComplicatedRecord] = sparkContext.parallelize((200 to 500).map(duplicateMapping), 1)
  val dupMatchScan2: SparkPlan = PhysicalRDD(complicatedAttributes, dupMatchRDD2)
  test ("duplicate output                     [Added by Daxi]"){
    val outputRDD = GeneralSymmetricHashJoin(Seq(complicatedAttributes(2)), Seq(complicatedAttributes(0)), dupMatchScan1, dupMatchScan2).execute()
    val seenValues: JavaArrayList[Row] = new JavaArrayList[Row]()

    outputRDD.collect().foreach(x => seenValues.add(x))
    //(0 to seenValues.size()-1).foreach(x=> println(seenValues.get(x)))
    val duplicateRow:Row = new JoinedRow(Row(999999, "I am duplicate", 999999), Row(999999, "I am duplicate", 999999))
    var countOccur:Int = 0
    assert(seenValues.contains(duplicateRow))
    for (i<-200 to 350){
      if (i%10!=0){
        assert(seenValues.contains(new JoinedRow(Row(i, "I am unique", i), Row(i, "I am unique", i))))
      }
    }

    for (j<-0 until seenValues.size()){
      if (seenValues.get(j)== duplicateRow){
        countOccur += 1
      }
    }
    assert(countOccur == 65)
  }

  val multiMatchRDD1: RDD[ComplicatedRecord] = sparkContext.parallelize((100 to 300).map(i => ComplicatedRecord(i, "Left", i%150)), 1)
  val multiMatchScan1: SparkPlan = PhysicalRDD(complicatedAttributes, multiMatchRDD1)
  val multiMatchRDD2: RDD[ComplicatedRecord] = sparkContext.parallelize((1 to 1000).map(i => ComplicatedRecord(i%150, "Right", i)), 1)
  val multiMatchScan2: SparkPlan = PhysicalRDD(complicatedAttributes, multiMatchRDD2)
  test ("multiple rows share the same key    [Added by Daxi](large)"){
    val outputRDD = GeneralSymmetricHashJoin(Seq(complicatedAttributes(2)), Seq(complicatedAttributes(0)), multiMatchScan1, multiMatchScan2).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()

    outputRDD.collect().foreach(x => seenValues = seenValues + x)
    //run slow but it's ok since this is just a test
    var countSize = 0
    for (i<-100 to 300){
      for (j<-1 to 1000){
        if (i%150 == j%150){
          countSize += 1
          assert(seenValues.contains(new JoinedRow(Row(i, "Left", i%150), Row(j%150, "Right", j))))
        }
      }
    }

    assert(countSize==seenValues.size)
  }

  val multiSMatchRDD1: RDD[ComplicatedRecord] = sparkContext.parallelize((1 to 10).map(i => ComplicatedRecord(i, "Left", i%2)), 1)
  val multiSMatchScan1: SparkPlan = PhysicalRDD(complicatedAttributes, multiSMatchRDD1)
  val multiSMatchRDD2: RDD[ComplicatedRecord] = sparkContext.parallelize((1 to 10).map(i => ComplicatedRecord(i%2, "Right", i)), 1)
  val multiSMatchScan2: SparkPlan = PhysicalRDD(complicatedAttributes, multiSMatchRDD2)
  test ("multiple rows share the same key     [Added by Daxi](small)"){
    val outputRDD = GeneralSymmetricHashJoin(Seq(complicatedAttributes(2)), Seq(complicatedAttributes(0)), multiSMatchScan1, multiSMatchScan2).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()

    outputRDD.collect().foreach(x => seenValues = seenValues + x)
    //run slow but it's ok since this is just a test
    var countSize = 0
    for (i<-1 to 10){
      for (j<-1 to 10){
        if (i%2 == j%2){
          countSize += 1
          assert(seenValues.contains(new JoinedRow(Row(i, "Left", i%2), Row(j%2, "Right", j))))
        }
      }
    }
    assert(countSize==50)
  }


  val diffRDD1: RDD[ComplicatedRecord] = sparkContext.parallelize((100 to 300).map(i => ComplicatedRecord(i*2, "Left", i)), 1)
  val diffScan1: SparkPlan = PhysicalRDD(complicatedAttributes, diffRDD1)
  val diffRDD2: RDD[ComplicatedRecord] = sparkContext.parallelize((200 to 350).map(i => ComplicatedRecord(i, "Right", i*2)), 1)
  val diffScan2: SparkPlan = PhysicalRDD(complicatedAttributes, diffRDD2)
  test ("no matching because of key            [Added by Daxi]"){
    val outputRDD = GeneralSymmetricHashJoin(Seq(complicatedAttributes(1)), Seq(complicatedAttributes(0)), diffScan1, diffScan2).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()

    outputRDD.collect().foreach(x => seenValues = seenValues + x)
    assert(seenValues.size == 0)
  }

  val noMatchRDD1: RDD[ComplicatedRecord] = sparkContext.parallelize((1 to 100).map(i => ComplicatedRecord(i, i.toString, i*2)), 1)
  val noMatchScan1: SparkPlan = PhysicalRDD(complicatedAttributes, noMatchRDD1)
  val noMatchRDD2: RDD[ComplicatedRecord] = sparkContext.parallelize((101 to 150).map(i => ComplicatedRecord(i, i.toString, i*2)), 1)
  val noMatchScan2: SparkPlan = PhysicalRDD(complicatedAttributes, noMatchRDD2)
  test ("no cross set                         [Added by Daxi]"){
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

































