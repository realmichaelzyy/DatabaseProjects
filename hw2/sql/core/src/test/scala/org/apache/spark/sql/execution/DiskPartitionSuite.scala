package org.apache.spark.sql.execution

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.FunSuite

class DiskPartitionSuite extends FunSuite {

  // TESTS FOR TASK #1

  test ("disk partition") {
    val partition: DiskPartition = new DiskPartition("disk partition test", 2000)

    for (i <- 1 to 500) {
      partition.insert(Row(i))
    }

    partition.closeInput()

    val data: Array[Row] = partition.getData.toArray
    (1 to 500).foreach((x: Int) => assert(data.contains(Row(x))))
  }

  test ("close input") {
    val partition: DiskPartition = new DiskPartition("close input test", 1)

    intercept[SparkException] {
      partition.getData()
    }

    partition.closeInput()

    intercept[SparkException] {
      partition.insert(Row(1))
    }
  }

  //The following tests are added by Daxi Li
  //test the behaviour of the interator returned when there is no input
  test ("no input") {
    val partition: DiskPartition = new DiskPartition("disk partition test", 2000)
    partition.closeInput()
    assert(partition.getData().hasNext == false)
  }

  //the only one write to disk is when closeInput() gets called
  test ("only one write to disk"){
    val partition: DiskPartition = new DiskPartition("disk partition test", 2000)
    partition.insert(Row(1))
    partition.closeInput()
    val data: Iterator[Row] = partition.getData()
    assert(data.hasNext == true)
    assert(data.next().equals(Row(1)))
    assert(data.hasNext == false)
  }

  test ("disk partition2, very small block size, basically one write for one row") {
    val partition: DiskPartition = new DiskPartition("disk partition test", 2)

    for (i <- 1 to 1000) {
      partition.insert(Row(i))
    }

    partition.closeInput()

    val data: Array[Row] = partition.getData.toArray
    (1 to 1000).foreach((x: Int) => assert(data.contains(Row(x))))
  }

  test ("disk partition2, each row usually 275, set as 276") {
    val partition: DiskPartition = new DiskPartition("disk partition test", 276)

    for (i <- 1 to 1000) {
      partition.insert(Row(i))
    }

    partition.closeInput()

    val data: Array[Row] = partition.getData.toArray
    (1 to 1000).foreach((x: Int) => assert(data.contains(Row(x))))
  }

  test ("disk partition, with supper large block size") {
    val partition: DiskPartition = new DiskPartition("disk partition test", 100000)

    for (i <- 1 to 1000) {
      partition.insert(Row(i))
    }

    partition.closeInput()

    val data: Array[Row] = partition.getData.toArray
    (1 to 1000).foreach((x: Int) => assert(data.contains(Row(x))))
  }
}
