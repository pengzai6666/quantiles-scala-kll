package quantiles

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest._

import scala.util.Random


class SimpleZoomRankTest extends FlatSpec with Matchers {
  "sketch" should "not crash or have a huge error for simple stream" in {
    for (k <- Array(1000)) {
      val qSketch = new QuantileNonSample[Int](k)
      val streamLen = 1000000
      val epsilon = 0.01
      val shrinkingFactor = 0.64
      val C = Math.pow(shrinkingFactor,2) * (2 * shrinkingFactor - 1)
      val prob = 2 * Math.exp(-C * Math.pow(epsilon,2) * Math.pow(k,2) * Math.pow(2, 2 * 0))



      // Choose A block or B block at a time and comment out the other

      // A block: test on random distribution
//      val shuffleList = Random.shuffle(List.range(1,streamLen))
//      shuffleList.foreach(item => qSketch.update(item))

      //  B block: test on zoom-in sketch: 1, n, 2, n-1, ...
      (1 to streamLen / 2).foreach { i =>
        qSketch.update(i)
        qSketch.update(streamLen + 1 - i)
      }

      var count = 0
      (1 to streamLen).foreach{ i =>
        val estimate_r = qSketch.getRank(i)
        val actual_r = i
        val diff_r = Math.abs(estimate_r - actual_r)
        println(s"estimate:${estimate_r}, actual:${actual_r}, diff:${diff_r}")
        if (diff_r >= epsilon * streamLen) count = count + 1
      }
      println(s"count: ${count}")
      val observe = count / streamLen.toDouble
      println(s"observe: ${observe}")
      println(s"prob:${prob}")
      println(s"constraints on violation count: ${prob * streamLen}")

      // test on actual items in compactors
      val itemsCount = qSketch.getCompactorItemsCount
      println(s"compactor item count : ${itemsCount}")

      // test on capacity of compactors
      val size = qSketch.getCompactorCapacityCount
      println(s"compactor capacity : ${size}")
      observe<=prob should be(true)
    }
  }

  "sketch" should "not crash or have a huge error for merged stream" in {
    for (k <- Array(1000)) {
      var lastMerge = new QuantileNonSample[Int](k)
      val partitionLen = 100000
      val merges = 10
      val epsilon = 0.01
      val shrinkingFactor = 0.64
      val C = Math.pow(shrinkingFactor,2) * (2 * shrinkingFactor - 1)
      val prob = 2 * Math.exp(-C * Math.pow(epsilon,2) * Math.pow(k,2) * Math.pow(2, 2 * 0))


      for (merge <- 0 until merges) {
        var newMerge = new QuantileNonSample[Int](k)
        (1 to partitionLen / 2).foreach { i =>
          newMerge.update(i+merge*partitionLen)
          newMerge.update(merge*partitionLen+partitionLen + 1 - i)
        }
        lastMerge = lastMerge.merge(newMerge)
      }
      var count = 0
      (1 to partitionLen * merges).foreach{ i =>
        val estimate_r = lastMerge.getRank(i)
        val actual_r = i
        val diff_r = Math.abs(estimate_r - actual_r)
        println(s"estimate:${estimate_r}, actual:${actual_r}, diff:${diff_r}")
        if (diff_r >= epsilon * partitionLen * merges) count = count + 1
      }
      println(s"count: ${count}")
      val observe = count / (partitionLen * merges).toDouble
      println(s"observe: ${observe}")
      println(s"prob:${prob}")

      // test on actual items in compactors
      val itemsCount = lastMerge.getCompactorItemsCount
      println(s"compactor item count : ${itemsCount}")

      // test on capacity of compactors
      val size = lastMerge.getCompactorCapacityCount
      println(s"compactor capacity : ${size}")
      observe<=prob should be(true)
    }
  }
}


