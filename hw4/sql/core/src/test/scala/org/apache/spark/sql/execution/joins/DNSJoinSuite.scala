package org.apache.spark.sql.execution.joins

import java.util.{ArrayList => JavaArrayList}
import scala.collection.mutable.HashSet
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, Row, Attribute}
import org.apache.spark.sql.execution.joins.dns.GeneralDNSJoin
import org.apache.spark.sql.execution.{PhysicalRDD, SparkPlan}
import org.apache.spark.sql.test.TestSQLContext._
import org.scalatest.FunSuite
import org.apache.spark.sql.execution.{Record, ComplicatedRecord, PhysicalRDD, SparkPlan}

import scala.util.Random

case class IP (ip: String)

class DNSJoinSuite extends FunSuite {
  val random: Random = new Random

  // initialize Spark magic stuff that we don't need to care about
  val sqlContext = new SQLContext(sparkContext)
  val IPAttributes: Seq[Attribute] = ScalaReflection.attributesFor[IP]

  var createdIPs: JavaArrayList[IP] = new JavaArrayList[IP]()

  import sqlContext.createSchemaRDD

  // initialize a SparkPlan that is a sequential scan over a small amount of data
  val smallRDD1: RDD[IP] = sparkContext.parallelize((1 to 100).map(i => {
    val ip: IP = IP((random.nextInt(220) + 1) + "." + random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256))
    createdIPs.add(ip)
    ip
  }), 1)
  val smallScan1: SparkPlan = PhysicalRDD(IPAttributes, smallRDD1)

  test ("simple dns join") {
    val outputRDD = GeneralDNSJoin(IPAttributes, IPAttributes, smallScan1, smallScan1).execute()

    val result = outputRDD.collect
    assert(result.length == 100)

    result.foreach(x => {
      val ip = IP(x.getString(0))
      assert(createdIPs contains ip)
      createdIPs remove ip
    })
  }


  //added by Daxi
  /*
  val recordAttributes: Seq[Attribute] = ScalaReflection.attributesFor[Record]
  val complicatedAttributes: Seq[Attribute] = ScalaReflection.attributesFor[ComplicatedRecord]



  val sameRDD1: RDD[ComplicatedRecord] = sparkContext.parallelize((1 to 1000).map(mapTwoIP), 1)
  val sameScan1: SparkPlan = PhysicalRDD(complicatedAttributes, sameRDD1)
  def mapTwoIP(i:Int) = {
    var ip:ComplicatedRecord = ComplicatedRecord(i, "144.86.80.118", i*2)
    if (i % 2 == 0){
      ip = ComplicatedRecord(1, "144.86.80.118", 1)
    }
    ip
  }


  test ("Eyeballing test, total request made"){
    val outputRDD = GeneralDNSJoin(Seq(complicatedAttributes(1)), Seq(complicatedAttributes(1)), sameScan1, sameScan1).execute()
    val result = outputRDD.collect
    assert(result.length == 1000)
    //println(IPAttributes)
  }*/ //Eyeballing section looks good
  val complicatedAttributes: Seq[Attribute] = ScalaReflection.attributesFor[ComplicatedRecord]
  val sameRDD1: RDD[ComplicatedRecord] = sparkContext.parallelize((1 to 1000).map(mapTwoIP), 1)
  val sameScan1: SparkPlan = PhysicalRDD(complicatedAttributes, sameRDD1)
  def mapTwoIP(i:Int) = {
    ComplicatedRecord(1, "58.55.187.8", i*2)
  }
  test("Row with same IP, different other attributes      [added by Daxi]"){
    val outputRDD = GeneralDNSJoin(Seq(complicatedAttributes(1)), Seq(complicatedAttributes(1)), sameScan1, sameScan1).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()
    outputRDD.collect().foreach(x => seenValues = seenValues + x)
    (1 to 1000).foreach(x=>seenValues.contains(Row(1,"58.55.182.8", x*2 ,"30.5801", "114.2734", "Wuhan,Hubei", "CN")))
  }

  val emptyRDD1: RDD[IP] = sparkContext.parallelize((0 until 0).map(i => {
    val ip: IP = IP(random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256))
    ip
  }), 1)
  val emptyScan1: SparkPlan = PhysicalRDD(IPAttributes, emptyRDD1)
  test("Empty IP set                                      [added by Daxi]"){
    val outputRDD = GeneralDNSJoin(IPAttributes, IPAttributes, emptyScan1, emptyScan1).execute()
    val result = outputRDD.collect

    assert(result.length == 0)
  }


  var singleIPs: HashSet[IP] = new HashSet[IP]()
  val singleRDD1: RDD[IP] = sparkContext.parallelize((0 until 1).map(i => {
    val ip: IP = IP(random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256))
    singleIPs += ip
    ip
  }), 1)
  val singleScan1: SparkPlan = PhysicalRDD(IPAttributes, singleRDD1)
  test("single IP                                         [added by Daxi]"){
    val outputRDD = GeneralDNSJoin(IPAttributes, IPAttributes, singleScan1, singleScan1).execute()
    val result = outputRDD.collect

    assert(result.length == 1)
    result.foreach(x => {
      val ip = IP(x.getString(0))
      assert(singleIPs contains ip)
    })
  }


  val randomRDD1: RDD[ComplicatedRecord] = sparkContext.parallelize((1 to 10).map(mapsomeIP), 1)
  val randomScan1: SparkPlan = PhysicalRDD(complicatedAttributes, randomRDD1)
  def mapsomeIP(i:Int) = {
    var result:ComplicatedRecord = null
    if (i%10==0) {
     result =  ComplicatedRecord(1, "58.55.187.8", i)
    }else if (i%10==1||i%10==2||i%10==3){
      result =  ComplicatedRecord(2, "208.99.212.153", i)//[208.99.212.153,47.6103,-122.3341,Seattle,Washington,US]
    }else{
      result = ComplicatedRecord(3, "15.188.170.85", i)//[15.188.170.85,37.3762,-122.1826,Palo Alto,California,US]
    }
    result
  }
  def resultAssertion(seenValues:HashSet[Row], i:Int) = {
    if (i%10==0) {
      assert(seenValues.contains(new JoinedRow(Row(1, "58.55.187.8", i, "30.5801", "114.2734", "Wuhan","Hubei", "CN"), Row())))
    }else if (i%10==1||i%10==2||i%10==3){
      assert(seenValues.contains(new JoinedRow(Row(2, "208.99.212.153", i, "47.6103", "-122.3341", "Seattle","Washington", "US"), Row())))//[208.99.212.153,47.6103,-122.3341,Seattle,Washington,US]
    }else{
      assert(seenValues.contains(new JoinedRow(Row(3, "15.188.170.85", i, "37.3762", "-122.1826", "Palo Alto","California", "US"), Row())))//[15.188.170.85,37.3762,-122.1826,Palo Alto,California,US]
    }
  }
  test("3 IPs, different other attributes                [added by Daxi]"){
    val outputRDD = GeneralDNSJoin(Seq(complicatedAttributes(1)), Seq(complicatedAttributes(1)), randomScan1, randomScan1).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()
    outputRDD.collect().foreach(x => seenValues = seenValues + x)
    (1 to 10).foreach(x=>resultAssertion(seenValues, x))
  }
//(2, "208.99.212.153", 1142, "47.6103", "-122.3341", "Seattle", "Washington", "US")
  //JoinedRow(2, "208.99.212.153", 1, "47.6103", "-122.3341", "Seattle", "Washington", "US")
  //JoinedRow("2", "208.99.212.153", 1, "47.6103", "-122.3341", "Seattle", "Washington", "US")
  //JoinedRow(1, "58.55.187.8", 10, "30.5801", "114.2734", "Wuhan,Hubei", "CN")
  //JoinedRow(1, "58.55.187.8", 10, "30.5801", "114.2734", "Wuhan", "Hubei", "CN")
  //JoinedRow(2, "208.99.212.153", 11, "47.6103", "-122.3341", "Seattle", "Washington", "US")
var lgIPs: JavaArrayList[IP] = new JavaArrayList[IP]()
  val largeRDD1: RDD[IP] = sparkContext.parallelize((1 to 2000).map(i => {
    val ip: IP = IP((random.nextInt(220) + 1) + "." + random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256))
    lgIPs.add(ip)
    ip
  }), 1)
  val largeScan1: SparkPlan = PhysicalRDD(IPAttributes, largeRDD1)

  test ("large dns join") {
    val outputRDD = GeneralDNSJoin(IPAttributes, IPAttributes, largeScan1, largeScan1).execute()

    val result = outputRDD.collect
    assert(result.length == 2000)

    result.foreach(x => {
      val ip = IP(x.getString(0))
      assert(lgIPs contains ip)
      lgIPs remove ip
    })
  }
}


















