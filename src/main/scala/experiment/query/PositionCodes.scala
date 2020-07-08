package experiment.query

import java.util.Random

import curve.XZPPositionCode

object PositionCodes {
  def main(args: Array[String]): Unit = {
    for (i <- 0 until 24) {
      val random = new Random(1000000)
      val randomLat = new Random(2661497)
      val lat = new Random(266149723)
      val lng = new Random(266149327)
      val sfc = XZPPositionCode.apply(i, 16.toShort)
      var sum = 0
      for (j <- 0 until 1000) {
        val lat1 = lat.nextDouble * 89
        val lng1 = lng.nextDouble * 179
        val offset = random.nextDouble * 0.1
        val offsetLat = randomLat.nextDouble * 0.1
        sum += sfc.ranges(lng1, lat1, lng1 + offset, lat1 + offsetLat).size
      }
      System.out.println(i + "," + sum)
    }
  }
}
