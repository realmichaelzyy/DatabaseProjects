package org.apache.spark.sql.execution.joins.dns

import java.util.{HashMap => JavaHashMap, ArrayList => JavaArrayList, Iterator => JavaIterator}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, Projection, Expression}
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable

/**
 * In this join, we are going to implement an algorithm similar to symmetric hash join.
 * However, instead of being provided with two input relations, we are instead going to
 * be using a single dataset and obtaining the other data remotely -- in this case by
 * asynchronous HTTP requests.
 *
 * The dataset that we are going to focus on reverse DNS, latitude-longitude lookups.
 * That is, given an IP address, we are going to try to obtain the geographical
 * location of that IP address. For this end, we are going to use a service called
 * telize.com, the owner of which has graciously allowed us to bang on his system.
 *
 * For that end, we have provided a simple library that makes asynchronously makes
 * requests to telize.com and handles the responses for you. You should read the
 * documentation and method signatures in DNSLookup.scala closely before jumping into
 * implementing this.
 *
 * The algorithm will work as follows:
 * We are going to be a bounded request buffer -- that is, we can only have a certain number
 * of unanswered requests at a certain time. When we initialize our join algorithm, we
 * start out by filling up our request buffer. On a call to next(), you should take all
 * the responses we have received so far and materialize the results of the join with those
 * responses and return those responses, until you run out of them. You then materialize
 * the next batch of joined responses until there are no more input tuples, there are no
 * outstanding requests, and there are no remaining materialized rows.
 *
 */
trait DNSJoin {
  self: SparkPlan =>

  val leftKeys: Seq[Expression]
  val left: SparkPlan

  override def output = left.output

  @transient protected lazy val leftKeyGenerator: Projection =
    newProjection(leftKeys, left.output)

  // How many outstanding requests we can have at once.
  val requestBufferSize: Int = 300

  /**
   * The main logic for DNS join. You do not need to implement anything outside of this method.
   * This method takes in an input iterator of IP addresses and returns a joined row with the location
   * data for each IP address.
   *
   * If you find the method definitions provided to be counter-intuitive or constraining, feel free to change them.
   * However, note that if you do:
   * 1. we will have a harder time helping you debug your code.
   * 2. Iterators must implement next and hasNext. If you do not implement those two methods, your code will not compile.
   *
   * **NOTE**: You should return JoinedRows, which take two input rows and returns the concatenation of them.
   * e.g., `new JoinedRow(row1, row2)`
   *
   * @param input the input iterator
   * @return the result of the join
   */
  def hashJoin(input: Iterator[Row]): Iterator[Row] = {
    new Iterator[Row] {
      // IMPLEMENT ME
      // Limit those 3 buffer to under requestBufferSize, well ResultBuffer may slightly pass the limit.
      val responseBuffer: ConcurrentHashMap[Int, Row] = new ConcurrentHashMap[Int, Row]()
      val requestBuffer: ConcurrentHashMap[Int, Row] = new ConcurrentHashMap[Int, Row]()
      val pairedResultBuffer: JavaArrayList[JoinedRow] = new JavaArrayList[JoinedRow]()

      val localCache: JavaHashMap[Row, JoinedRow] = new JavaHashMap[Row, JoinedRow]()

      //to save memory
      var circleID = 0


      /**
       * This method returns the next joined tuple.
       *
       * *** THIS MUST BE IMPLEMENTED FOR THE ITERATOR TRAIT ***
       */
      override def next() = {
        // IMPLEMENT ME
        var result:Row = null

        if (hasNext()){
          result = pairedResultBuffer.remove(0)
        }

        result
      }

      /**
       * This method returns whether or not this iterator has any data left to return.
       *
       * *** THIS MUST BE IMPLEMENTED FOR THE ITERATOR TRAIT ***
       */
      override def hasNext() = {
        // IMPLEMENT ME
        fillRequestBuffer()
        while (requestBuffer.size() != 0 && pairedResultBuffer.size() == 0) {
          checkAndCleanBuffer()
        }

        pairedResultBuffer.size() != 0
      }


      /**
       * This method takes the next element in the input iterator and makes an asynchronous request for it.
       */
      private def makeRequest(inputRow: Row) = {
        // IMPLEMENT ME
        if (requestBuffer.size() < requestBufferSize) {
          val keyIp:Row = leftKeyGenerator(inputRow)
          while (requestBuffer.containsKey(circleID)){
            circleID = (circleID + 1) % requestBufferSize
          }
          requestBuffer.put(circleID, inputRow)
          DNSLookup.lookup(circleID, keyIp.getString(0), responseBuffer, requestBuffer)
        }
      }

      private def fillRequestBuffer() ={
        while (requestBuffer.size()<requestBufferSize && input.hasNext){
          val item: Row = input.next()
          val key: Row = leftKeyGenerator(item)
          if (localCache.containsKey(key)){
            pairedResultBuffer.add(new JoinedRow(item, localCache.get(key)))
          }else{
            makeRequest(item)
          }
        }
      }

      private def checkAndCleanBuffer() ={
        //clear up buffer => cache, and prepare for output list
        //fill up output list
        while (pairedResultBuffer.size() < requestBufferSize && requestBuffer.size() > 0 && responseBuffer.size() > 0){
          val keys:JavaIterator[Int] = responseBuffer.keySet().iterator()
          while (keys.hasNext()){
            val key = keys.next()
            val rowResponse: Row = responseBuffer.get(key)
            val rowRequest: Row = requestBuffer.get(key)
            val result: JoinedRow = new JoinedRow(rowRequest, rowResponse)
            requestBuffer.remove(key)
            responseBuffer.remove(key)
            localCache.put(leftKeyGenerator(rowRequest), result)
            pairedResultBuffer.add(result)
          }
        }
      }
    }
  }
}









