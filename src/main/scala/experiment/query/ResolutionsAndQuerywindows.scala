package experiment.query

import java.util

import curve.{XZ2SFC, XZPlusSFC}
import org.locationtech.sfcurve.IndexRange
import random.MinimumBoundingBox
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.JavaConverters._
import scala.collection.Seq

object ResolutionsAndQuerywindows {

  def main(args: Array[String]): Unit = {
    val table = args(0)
    val sp = args(1).toShort
    val ep = args(2).toShort
    val host = args(3)
    val MBR = args(4).split(",")
    //    var minLon: Double = 116.28172
    //    var minLat: Double = 39.79123
    //    var maxLon: Double = 116.28672
    //    var maxLat: Double = 39.79623
    //    val interval: Double = 0.005
    //    val offset: Double = 0.001
    var minLon: Double = MBR(0).toDouble
    var minLat: Double = MBR(1).toDouble
    var maxLonT: Double = MBR(2).toDouble
    var maxLatT: Double = MBR(3).toDouble
    //    var minLon: Double = 116.34557
    //    var minLat: Double = 39.92818
    //    var maxLon: Double = 116.35057
    //    var maxLat: Double = 39.93318
    val interval: Double = 0.01
    var offset: Double = 0.01

    val jedis: Jedis = new Jedis(host, 6379, 10000)
    //query(table, minLon, minLat, maxLon, maxLat, offset * 1, xzPlusSFC, jedis)
    for (j <- sp to ep) {
      val xzPlusSFC: XZPlusSFC = XZPlusSFC.apply(j.toShort)
      println(s"---precision:$j----")
      var maxLon = maxLonT
      var maxLat = maxLatT
      for (i <- 1 to 10) {
        //println(s"$minLon, $minLat, $maxLon, $maxLat")
        //jedis.zrangeByScore(table + s"_$j", 1L, 1L)
        query(table + s"_$j", minLon, minLat, minLon + 0.001, minLat + 0.001, 0.001, offset, xzPlusSFC, jedis, log = false)
        println(s"---xzp:$i----")
        query(table + s"_$j", minLon, minLat, maxLon, maxLat, interval * i, offset, xzPlusSFC, jedis)
        //      minLon = maxLon
        //      minLat = maxLat
        //      maxLon += i * interval
        //      maxLat += i * interval
        //maxLon += interval
        //maxLat += interval
        //offset += offset
      }
    }
    jedis.close()
  }

  def query(table: String, minLon: Double, minLat: Double, maxLon: Double, maxLat: Double, interval: Double, offset: Double, sfc: XZ2SFC, jedis: Jedis, log: Boolean = true) = {
    for (i <- -3 to 3) {
      for (j <- -3 to 3) {
        val mbr1 = new MinimumBoundingBox(minLon + offset * i, minLat + offset * j, minLon + offset * i + interval, minLat + offset * j + interval)
        val xzRanges: Seq[IndexRange] = sfc.ranges(minLon + offset * i, minLat + offset * j, minLon + offset * i + interval, minLat + offset * j + interval)
        val pLine: Pipeline = jedis.pipelined
        val pLineNC: Pipeline = jedis.pipelined
        val t = System.currentTimeMillis()
        for (elem <- xzRanges) {
          if (!elem.contained) {
            pLineNC.zrangeByScore(table, elem.lower, elem.upper);
          } else {
            pLine.zrangeByScore(table, elem.lower, elem.upper);
          }
        }
        val listNC = pLineNC.syncAndReturnAll()
        val list = pLine.syncAndReturnAll()
        var size = 0
        var querySize = 0
        for (o <- listNC.toArray()) {
          val value = o.asInstanceOf[util.LinkedHashSet[String]]
          querySize += value.size()
          for (v <- value.asScala) {
            val polygon = v.split("_")(0)
            curve.WKTUtils.read(polygon)
            //size += 1
            if (mbr1.intersects(curve.WKTUtils.read(polygon).getEnvelopeInternal)) {
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
