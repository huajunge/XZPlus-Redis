package random

import java.util

import com.urbancomputing.geomesa.model.basic.box.MinimumBoundingBox
import curve.{HBPlusSFC, XZ2SFC, XZPlusSFC}
import org.locationtech.geomesa.utils.interop.WKTUtils
import org.locationtech.sfcurve.IndexRange
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.JavaConverters._
import scala.collection.Seq

object PrecisionWithAlg {

  def main(args: Array[String]): Unit = {
    val table = args(0)
    val tableHB = args(1)
    val tableXZ2 = args(2)
    val sp = args(3).toShort
    val ep = args(4).toShort
    val host = args(5)
    val MBR = args(6).split(",")
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
    val offset: Double = 0.03

    val jedis: Jedis = new Jedis(host, 6379, 10000)
    //query(table, minLon, minLat, maxLon, maxLat, offset * 1, xzPlusSFC, jedis)
    for (j <- sp to ep) {
      val xzPlusSFC: XZPlusSFC = XZPlusSFC.apply(j.toShort)
      val hb: HBPlusSFC = HBPlusSFC.apply(j.toShort)
      val xz2: XZ2SFC = XZ2SFC.apply(j.toShort)

      println(s"---precision:$j----")
      query(table + s"_$j", minLon, minLat, minLon + 0.001, minLat + 0.001, offset, xzPlusSFC, jedis, log = false)
      println(s"---xzp:----")
      query(table + s"_$j", minLon, minLat, maxLon, maxLat, offset, xzPlusSFC, jedis)
      println(s"---hbp:----")
      query(tableHB + s"_$j", minLon, minLat, maxLon, maxLat, offset, hb, jedis)
      println(s"---xz2:----")
      query(tableXZ2 + s"_$j", minLon, minLat, maxLon, maxLat, offset, xz2, jedis)
    }
    jedis.close()
  }

  def query(table: String, minLon: Double, minLat: Double, maxLon: Double, maxLat: Double, offset: Double, sfc: XZ2SFC, jedis: Jedis, log: Boolean = true) = {
    for (i <- -1 to 1) {
      for (j <- -1 to 1) {
        val mbr1 = new MinimumBoundingBox(minLon + offset * i, minLat + offset * j, maxLon + offset * i, maxLat + offset * j)
        val xzRanges: Seq[IndexRange] = sfc.ranges(minLon + offset * i, minLat + offset * j, maxLon + offset * i, maxLat + offset * j)
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
        if (log) {
          println(s"$queryTime,$querySize,$size,${xzRanges.size}")
        }
        pLineNC.close()
        pLine.close()
      }
    }
    /*for (j <- 0 to 9) {
      val mbr1 = new MinimumBoundingBox(minLon + offset * j, minLat + offset * j, maxLon + offset * j, maxLat + offset * j)
      val xzRanges: Seq[IndexRange] = sfc.ranges(minLon + offset * j, minLat + offset * j, maxLon + offset * j, maxLat + offset * j)
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
      println(s"$queryTime,$querySize,$size")
      pLineNC.close()
      pLine.close()
    }*/
  }
}
