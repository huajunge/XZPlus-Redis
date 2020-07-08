package random

import curve.{HBPlusSFC, XZ2SFC, XZPlusSFC}
import redis.clients.jedis.Jedis

object QueryDataSize {

  def main(args: Array[String]): Unit = {
    val table = args(0)
    val tableHB = args(1)
    val tablexz = args(2)
    val precision = args(3).toShort
    val sp = args(4).toShort
    val ep = args(5).toShort
    val host = args(6)
    val MBR = args(7).split(",")
    //    var minLon: Double = 116.28172
    //    var minLat: Double = 39.79123
    //    var maxLon: Double = 116.28672
    //    var maxLat: Double = 39.79623
    //    val interval: Double = 0.005
    //    val offset: Double = 0.001

    val jedis: Jedis = new Jedis(host, 6379, 5000)
    var minLon: Double = MBR(0).toDouble
    var minLat: Double = MBR(1).toDouble
    var maxLon: Double = MBR(2).toDouble
    var maxLat: Double = MBR(3).toDouble
    var offset: Double = 0.03
    var interval: Double = 0.03
    val xzPlusSFC: XZPlusSFC = XZPlusSFC.apply(precision)
    val hb: HBPlusSFC = HBPlusSFC.apply(precision)
    val xz2SFC: XZ2SFC = XZ2SFC.apply(precision)
    for (p <- sp to ep) {
      println(s"---data_size:$p----")
      //      var minLon: Double = 116.34557
      //      var minLat: Double = 39.92818
      //      var maxLon: Double = 116.37057
      //      var maxLat: Double = 39.95318
      QueryWindowWithPrecision.query(table, minLon, minLat, minLon + 0.0001, minLat + 0.0001, interval, offset, xzPlusSFC, jedis, false)
      println(s"---xzp:$p----")
      QueryWindowWithPrecision.query(s"${table}_$p", minLon, minLat, maxLon, maxLat, interval, offset, xzPlusSFC, jedis)
      println(s"---hb:$p----")
      QueryWindowWithPrecision.query(s"${tableHB}_$p", minLon, minLat, maxLon, maxLat, interval, offset, hb, jedis)
      println(s"---xz:$p----")
      QueryWindowWithPrecision.query(s"${tablexz}_$p", minLon, minLat, maxLon, maxLat, interval, offset, xz2SFC, jedis)

    }
    jedis.close()
  }
}
