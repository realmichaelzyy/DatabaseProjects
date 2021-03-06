package org.apache.spark.sql.execution.joins.dns

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Expression, JoinedRow, Projection}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BuildSide
import org.apache.spark.util.collection.CompactBuffer

import scala.collection.mutable
import scala.collection.mutable.{HashMap}
import java.util.{ArrayList => JavaArrayList}
/**
 * ***** TASK 1 ******
 *
 * Symmetric hash join is a join algorithm that is designed primarily for data streams.
 * In this algorithm, you construct a hash table for _both_ sides of the join, not just
 * one side as in regular hash join.
 *
 * We will be implementing a version of symmetric hash join that works as follows:
 * We will start with the "left" table as the inner table and "right" table as the outer table
 * and begin by streaming tuples in from the inner relation. For every tuple we stream in
 * from the inner relation, we insert it into its corresponding hash table. We then check if
 * there are any matching tuples in the other relation -- if there are, then we join this
 * tuple with the corresponding matches. Otherwise, we switch the inner
 * and outer relations -- that is, the old inner becomes the new outer and the old outer
 * becomes the new inner, and we proceed to repeat this algorithm, streaming from the
 * new inner.
 */
trait SymmetricHashJoin {
  self: SparkPlan =>

  val leftKeys: Seq[Expression]
  val rightKeys: Seq[Expression]
  val left: SparkPlan
  val right: SparkPlan

  override def output = left.output ++ right.output

  @transient protected lazy val leftKeyGenerator: Projection =
    newProjection(leftKeys, left.output)

  @transient protected lazy val rightKeyGenerator: Projection =
    newProjection(rightKeys, right.output)

  /**
   * The main logic for symmetric hash join. You do not need to worry about anything other than this method.
   * This method takes in two iterators (one for each of the input relations), and returns an iterator (
   * representing the result of the join).
   *
   * If you find the method definitions provided to be counter-intuitive or constraining, feel free to change them.
   * However, note that if you do:
   *  1. we will have a harder time helping you debug your code.
   *  2. Iterators must implement next and hasNext. If you do not implement those two methods, your code will not compile.
   *
   * **NOTE**: You should return JoinedRows, which take two input rows and returns the concatenation of them.
   * e.g., `new JoinedRow(row1, row2)`
   *
   * @param leftIter an iterator for the left input
   * @param rightIter an iterator for th right input
   * @return iterator for the result of the join
   */
  protected def symmetricHashJoin(leftIter: Iterator[Row], rightIter: Iterator[Row]): Iterator[Row] = {
    new Iterator[Row] {
      /* Remember that Scala does not have any constructors. Whatever code you write here serves as a constructor. */
      // IMPLEMENT ME
      //variable for recording states
      val pairedResultBuffer: JavaArrayList[JoinedRow] = new JavaArrayList[JoinedRow]()
      var isLeftCurrentStream:Boolean = false
      var currentStream: Iterator[Row] = rightIter
      var currentKeyGenerator: Projection =  rightKeyGenerator
      var tableForLookUp: HashMap[Row, CompactBuffer[Row]] =  new HashMap[Row, CompactBuffer[Row]]()
      var tableForInsertion: HashMap[Row, CompactBuffer[Row]] = new HashMap[Row, CompactBuffer[Row]]()

      /**
       * This method returns the next joined tuple.
       *
       * *** THIS MUST BE IMPLEMENTED FOR THE ITERATOR TRAIT ***
       */
      override def next() = {
        // IMPLEMENT ME
        if (!hasNext()){
          throw new NoSuchElementException
        }
        pairedResultBuffer.remove(0)
      }

      /**
       * This method returns whether or not this iterator has any data left to return.
       *
       * *** THIS MUST BE IMPLEMENTED FOR THE ITERATOR TRAIT ***
       */
      override def hasNext() = {
        // IMPLEMENT ME
        findNextMatch()
        !pairedResultBuffer.isEmpty()
      }

      /**
       * This method is intended to switch the inner & outer relations.
       */

      private def switchRelations() = {
        // IMPLEMENT ME
        //will not switch to an empty stream
        if ((isLeftCurrentStream&&rightIter.hasNext)||(!isLeftCurrentStream&&leftIter.hasNext)) {
          if (isLeftCurrentStream) {
            currentStream = rightIter
            currentKeyGenerator = rightKeyGenerator
          } else {
            currentStream = leftIter
            currentKeyGenerator = leftKeyGenerator
          }
          isLeftCurrentStream = !isLeftCurrentStream

          val temp: HashMap[Row, CompactBuffer[Row]] = tableForLookUp
          tableForLookUp = tableForInsertion
          tableForInsertion = temp
        }//otherwise no need to switch
      }

      def joinRowWithOrder(streamIn: Row, lookupResults: CompactBuffer[Row]) = {
        if (isLeftCurrentStream){
          lookupResults.foreach(x=>pairedResultBuffer.add(new JoinedRow(streamIn, x)))
        }else{
          lookupResults.foreach(x=>pairedResultBuffer.add(new JoinedRow(x, streamIn)))
        }
      }

      /**
       * This method is intended to find the next match and return true if one such match exists.
       *
       * @return whether or not a match was found
       */

      def findNextMatch() = {
        // IMPLEMENT ME
        if (leftIter!=null&&rightIter!=null){
          while (pairedResultBuffer.isEmpty() && (leftIter.hasNext || rightIter.hasNext)){
            switchRelations()
            val streamIn:Row = currentStream.next()
            val key: Row = currentKeyGenerator(streamIn)

            val insertResult:Option[CompactBuffer[Row]] = tableForInsertion.get(key)
            if (insertResult != None){
              (insertResult.get)+=(streamIn)
            }else{
              tableForInsertion.put(key, (new CompactBuffer[Row]())+=streamIn)
            }

            val lookupResult:Option[CompactBuffer[Row]] = tableForLookUp.get(key)
            if (lookupResult != None){
              joinRowWithOrder(streamIn, lookupResult.get)
            }
          }
        }
      }
    }
  }
}