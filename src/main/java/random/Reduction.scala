package random

import java.util
import java.util.Random

import com.urbancomputing.geomesa.model.basic.box.MinimumBoundingBox
import curve.{HBPlusSFC, XZ2SFC, XZPlusSFC}
import org.locationtech.geomesa.utils.interop.WKTUtils
import org.locationtech.sfcurve.IndexRange
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.JavaConverters._
import scala.collection.Seq

object Reduction {
  def main(args: Array[String]): Unit = {
    val table = args(0)
    val tablexz = args(1)
    val tableHB = args(2)
    val precision = args(3).toShort
    val host = args(4)
    val MBR = args(5).split(",")
    //    var minLon: Double = 116.28172
    //    var minLat: Double = 39.79123
    //    var maxLon: Double = 116.28672
    //    var maxLat: Double = 39.79623
    //    val interval: Double = 0.005
    //    val offset: Double = 0.001
    var minLon: Double = MBR(0).toDouble
    var minLat: Double = MBR(1).toDouble
    var maxLon: Double = MBR(2).toDouble
    var maxLat: Double = MBR(3).toDouble
    //    var minLon: Double = 116.34557
    //    var minLat: Double = 39.92818
    //    var maxLon: Double = 116.35057
    //    var maxLat: Double = 39.93318
    val interval: Double = 0.01
    var offset: Double = 0.05
    val xzPlusSFC: XZPlusSFC = XZPlusSFC.apply(precision)
    val xz2SFC: XZ2SFC = XZ2SFC.apply(precision)
    val hb: HBPlusSFC = HBPlusSFC.apply(precision)
    val jedis: Jedis = new Jedis(host, 6379, 10000)
    var random = new Random(1000000)
    var offsetRD = new Random(2661497)
    println(s"---xzp:----")
    for (i <- 1 to 1000) {
      val lat = random.nextDouble() * 0.1
      val lng = random.nextDouble() * 0.1
      val oflat = offsetRD.nextDouble() * 0.1
      val oflng = offsetRD.nextDouble() * 0.1
      query(table, minLon + oflng, minLat + oflat, minLon + oflng + lng, minLat + oflat + lat, xzPlusSFC, jedis)
    }

    random = new Random(1000000)
    offsetRD = new Random(2661497)
    println(s"---xz:----")
    for (i <- 1 to 1000) {
      val lat = random.nextDouble() * 0.1
      val lng = random.nextDouble() * 0.1
      val oflat = offsetRD.nextDouble() * 0.1
      val oflng = offsetRD.nextDouble() * 0.1
      query(tablexz, minLon + oflng, minLat + oflat, minLon + oflng + lng, minLat + oflat + lat, xz2SFC, jedis)
    }
  }

  def query(table: String, minLon: Double, minLat: Double, maxLon: Double, maxLat: Double, sfc: XZ2SFC, jedis: Jedis) = {
    val mbr1 = new MinimumBoundingBox(minLon, minLat, maxLon, maxLat)
    val xzRanges: Seq[IndexRange] = sfc.ranges(minLon, minLat, maxLon, maxLat)
    val pLine: Pipeline = jedis.pipelined
    val pLineNC: Pipeline = jedis.pipelined
    for (elem <- xzRanges) {
      if (!elem.contained) {
        pLineNC.zrangeByScore(table, elem.lower, elem.upper);
      } else {
        pLine.zrangeByScore(table, elem.lower, elem.upper);
      }
    }
    val t = System.currentTimeMillis()
    val listNC = pLineNC.syncAndReturnAll()
    val list = pLine.syncAndReturnAll()
    var size = 0
    var querySize = 0
    for (o <- listNC.toArray()) {
      val value = o.asInstanceOf[util.LinkedHashSet[String]]
      querySize += value.size()
      for (v <- value.asScala) {
        val polygon = v.split("_")(0)
        WKTUtils.read(polygon)
        //size += 1
        if (mbr1.intersects(WKTUtils.read(polygon).getEnvelopeInternal)) {
          size += 1
        }
      }
    }
    val queryTime = System.currentTimeMillis() - t
    for (o <- list.toArray()) {
      val value = o.asInstanceOf[util.LinkedHashSet[String]]
      size += value.size()
      querySize += value.size()
    }
    println(s"$queryTime,$querySize,$size,${xzRanges.size}")
    pLineNC.close()
    pLine.close()
  }
}
