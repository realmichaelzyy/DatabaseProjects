package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.joins.dns.GeneralDNSJoin
import org.apache.spark.sql.execution.{PhysicalRDD, SparkPlan}
import org.apache.spark.sql.test.TestSQLContext._
import org.scalatest.FunSuite
import org.apache.spark.sql.execution.{Record, ComplicatedRecord, PhysicalRDD, SparkPlan}

import scala.collection.mutable.HashSet
import scala.util.Random

case class IP (ip: String)
case class ReverseDNS

class DNSJoinSuite extends FunSuite {
  val random: Random = new Random

  // initialize Spark magic stuff that we don't need to care about
  val sqlContext = new SQLContext(sparkContext)
  val IPAttributes: Seq[Attribute] = ScalaReflection.attributesFor[IP]

  var createdIPs: HashSet[IP] = new HashSet[IP]()

  import sqlContext.createSchemaRDD

  // initialize a SparkPlan that is a sequential scan over a small amount of data
  val smallRDD1: RDD[IP] = sparkContext.parallelize((1 to 100).map(i => {
    val ip: IP = IP(random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256))
    createdIPs += ip
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
  test("Row with same IP, different other attributes"){
    val outputRDD = GeneralDNSJoin(Seq(complicatedAttributes(1)), Seq(complicatedAttributes(1)), sameScan1, sameScan1).execute()
    val result = outputRDD.collect
  }

}


















