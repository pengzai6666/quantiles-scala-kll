package quantiles

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.Sorting.quickSort


class UniformGaussianRankTest extends FlatSpec with Matchers {
  "sketch" should "not crash or have a huge error for simple stream" in {
    for (k <- Array(1000)) {
      val qSketch = new QuantileNonSample[Int](k)
      val streamLen = 10000
      val epsilon = 0.01
      val shrinkingFactor = 0.64
      val C = Math.pow(shrinkingFactor, 2) * (2 * shrinkingFactor - 1)
      val prob = 2 * Math.exp(-C * Math.pow(epsilon, 2) * Math.pow(k, 2) * Math.pow(2, 2 * 0))

      // Choose A block or B block at a time and comment out the other

      // A block: test on uniform distribution
      var store = ArrayBuffer[Int]()

      for (i <- 0 to streamLen) {
        val tmp = Random.nextInt(streamLen)
        store = store :+ tmp
        qSketch.update(tmp)
      }

//      // B block: test on Gaussian distribution
//      for (i <- 0 to streamLen){
//        val tmp = (Random.nextGaussian() * streamLen).toInt
//        store = store :+ tmp
//        qSketch.update(tmp)
//      }

      val finalStore = store.toArray
      quickSort(finalStore)

      // test on actual items in compactors
      val itemsCount = qSketch.getCompactorItemsCount
      // test on capacity of compactors
      val size = qSketch.getCompactorCapacityCount

      var count = 0
      finalStore.foreach { i =>
        val estimate_r = qSketch.getRank(i)
        val actual_r = finalStore.indexOf(i) + 1
        val diff_r = Math.abs(estimate_r - actual_r)
        println(s"estimate:${estimate_r}, actual:${actual_r}, diff:${diff_r}")
        if (diff_r >= epsilon * streamLen) count = count + 1
      }
      println(s"count: ${count}")
      val observe = count / streamLen.toDouble
      println(s"observe: ${observe}")
      println(s"prob:${prob}")
      println(s"constraints on violation count: ${prob * streamLen}")
      println(s"compactor item count : ${itemsCount}")
      println(s"compactor capacity : ${size}")
      observe <= prob should be(true)
    }
  }
}
