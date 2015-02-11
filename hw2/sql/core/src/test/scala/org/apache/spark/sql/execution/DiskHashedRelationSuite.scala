package org.apache.spark.sql.execution

import java.util.{ArrayList => JavaArrayList}
import org.apache.spark.sql.catalyst.expressions.{Projection, Row}
import org.scalatest.FunSuite

import scala.collection.mutable.ArraySeq

class DiskHashedRelationSuite extends FunSuite {

  private val keyGenerator = new Projection {
    override def apply(row: Row): Row = row
  }

  // TESTS FOR TASK #2
  test("values are in correct partition") {
    val data: Array[Row] = (0 to 100).map(i => Row(i)).toArray
    val hashedRelation: DiskHashedRelation = DiskHashedRelation(data.iterator, keyGenerator, 3, 64000)
    var count: Int = 0

    for (partition <- hashedRelation.getIterator()) {
      for (row <- partition.getData()) {
        assert(row.hashCode() % 3 == count)
      }
      count += 1
    }
    hashedRelation.closeAllPartitions()
  }

  test ("empty input") {
    val data: ArraySeq[Row] = new ArraySeq[Row](0)
    val hashedRelation: DiskHashedRelation = DiskHashedRelation(data.iterator, keyGenerator)

    for (partition <- hashedRelation.getIterator()) {
      assert(!partition.getData.hasNext)
    }

    hashedRelation.closeAllPartitions()
  }


  //Tests added by Daxi Li

  //to make sure no file conflicts
  test("Multiple hashedPartition") {
    val data: Array[Row] = (0 to 100).map(i => Row(i)).toArray
    val hashedRelation: DiskHashedRelation = DiskHashedRelation(data.iterator, keyGenerator, 3, 64000)

    val data1: Array[Row] = (101 to 200).map(i => Row(i)).toArray
    val hashedRelation1: DiskHashedRelation = DiskHashedRelation(data1.iterator, keyGenerator, 3, 64000)

    for (partition <- hashedRelation.getIterator()) {
      for (row <- partition.getData()) {
        assert(data.contains(row))
        assert(!data1.contains(row))
      }
    }

    for (partition1 <- hashedRelation1.getIterator()) {
      for (row1 <- partition1.getData()) {
        assert(data1.contains(row1))
        assert(!data.contains(row1))
      }
    }
  }

  test("Intend to pass not allowed number"){
    val data: Array[Row] = (0 to 100).map(i => Row(i)).toArray
    val hashedRelation: DiskHashedRelation = DiskHashedRelation(data.iterator, keyGenerator, 0, 0)
    assert(hashedRelation==null)

    val data1: Array[Row] = (0 to 100).map(i => Row(i)).toArray
    val hashedRelation1: DiskHashedRelation = DiskHashedRelation(data1.iterator, keyGenerator, 0, 10)
    assert(hashedRelation1==null)

    val data2: Array[Row] = (0 to 100).map(i => Row(i)).toArray
    val hashedRelation2: DiskHashedRelation = DiskHashedRelation(data2.iterator, keyGenerator, 10, 0)
    assert(hashedRelation2==null)

    val hashedRelation3: DiskHashedRelation = DiskHashedRelation(null, keyGenerator, 10, 0)
    assert(hashedRelation3==null)
  }

  test("Only one partition") {
    val data: Array[Row] = (0 to 10000).map(i => Row(i)).toArray
    val hashedRelation: DiskHashedRelation = DiskHashedRelation(data.iterator, keyGenerator, 1, 64000)
    var total: Int = 0
    var partitionNum:Int = 0
    val temp_list: JavaArrayList[Row] = new JavaArrayList[Row]


    for (partition <- hashedRelation.getIterator()) {
      for (row <- partition.getData()) {
        total+=1
        temp_list.add(row)
      }
      partitionNum+=1
    }

    for (i<-0 to 10000){
      if (temp_list.contains(Row(i)) != true)
        assert(false)
    }

    assert(partitionNum==1)
    assert(total==10001)
    hashedRelation.closeAllPartitions()
  }

  test("HashToOnePartition"){
    val data: Array[Row] = (0 to 1000).map(i => Row(1)).toArray
    val hashedRelation: DiskHashedRelation = DiskHashedRelation(data.iterator, keyGenerator, 5, 64000)
    var hitYet:Boolean = false
    var total: Int = 0

    for (partition <- hashedRelation.getIterator()) {
      val data = partition.getData()
      if (data.hasNext&&hitYet){ //shouldn't get in this block
        assert(false)
      }
      while(data.hasNext){
        total += 1
        hitYet = true
        data.next()
      }
    }
    assert(total==1001)
  }
}