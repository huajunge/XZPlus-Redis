package random

import java.util

import com.urbancomputing.geomesa.model.basic.box.MinimumBoundingBox
import curve.{HBPlusSFC, NativeSFC, XZ2SFC, XZPlusSFC, ZOneValueSFC}
import org.locationtech.geomesa.utils.interop.WKTUtils
import org.locationtech.sfcurve.IndexRange
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.JavaConverters._
import scala.collection.Seq

object QueryFromRedis {

  def main(args: Array[String]): Unit = {
    val table = args(0)
    val tablexz = args(1)
    val tableHB = args(2)

    val precision = args(3).toShort
    val host = args(4)
    val MBR = args(5).split(",")
    val tableZone = args(6)
    val tableNative = args(7)
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
    var offset: Double = 0.01
    val xzPlusSFC: XZPlusSFC = XZPlusSFC.apply(precision)
    val xz2SFC: XZ2SFC = XZ2SFC.apply(precision)
    val hb: HBPlusSFC = HBPlusSFC.apply(precision)
    val zone: ZOneValueSFC = ZOneValueSFC.apply(precision)
    val native: NativeSFC = NativeSFC.apply(precision)

    val jedis: Jedis = new Jedis(host, 6379, 10000)
    //query(table, minLon, minLat, maxLon, maxLat, offset * 1, xzPlusSFC, jedis)
    for (i <- 1 to 10) {
      println(s"$minLon, $minLat, $maxLon, $maxLat")
      //Thread.sleep(10);
      query(table, minLon, minLat, minLon + 0.001, minLat + 0.001, 0.001, offset, xzPlusSFC, jedis, false);
      println(s"---xzp:$i----")
      query(table, minLon, minLat, maxLon, maxLat, interval * i, offset, xzPlusSFC, jedis)
      //Thread.sleep(10);
      println(s"---xz:$i----")
      query(tablexz, minLon, minLat, maxLon, maxLat, interval * i, offset, xz2SFC, jedis)
      //Thread.sleep(10);
      println(s"---hb:$i----")
      query(tableHB, minLon, minLat, maxLon, maxLat, interval * i, offset, hb, jedis)
      //Thread.sleep(10);
      println(s"---zone:$i----")
      query(tableZone, minLon, minLat, maxLon, maxLat, interval * i, offset, zone, jedis)
      //Thread.sleep(10);
      println(s"---native:$i----")
      queryNative(tableNative, minLon, minLat, interval * i, offset, native, jedis)
      //      minLon = maxLon
      //      minLat = maxLat
      //      maxLon += i * interval
      //      maxLat += i * interval

      maxLon += interval
      maxLat += interval
      //offset += offset
    }
    jedis.close()
  }

  def query(table: String, minLon: Double, minLat: Double, maxLon: Double, maxLat: Double, interval: Double, offset: Double, sfc: XZ2SFC, jedis: Jedis, flag: Boolean = true): Unit = {
    for (i <- -3 to 3) {
      for (j <- -3 to 3) {
        val mbr1 = new MinimumBoundingBox(minLon + offset * i, minLat + offset * j, minLon + offset * i + interval, minLat + offset * j + interval)
        val t = System.currentTimeMillis()
        val xzRanges: Seq[IndexRange] = sfc.ranges(minLon + offset * i, minLat + offset * j, minLon + offset * i + interval, minLat + offset * j + interval)
        val pLine: Pipeline = jedis.pipelined
        val pLineNC: Pipeline = jedis.pipelined
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
            WKTUtils.read(polygon)
            //size += 1
            if (mbr1.intersects(WKTUtils.read(polygon).getEnvelopeInternal)) {
              size += 1
            }
          }
        }
        for (o <- list.toArray()) {
          val value = o.asInstanceOf[util.LinkedHashSet[String]]
          size += value.size()
          querySize += value.size()
        }
        val queryTime = System.currentTimeMillis() - t

        if(flag) {
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

  def queryNative(table: String, minLon: Double, minLat: Double, interval: Double, offset: Double, sfc: NativeSFC, jedis: Jedis) = {
    for (i <- -3 to 3) {
      for (j <- -3 to 3) {
        val mbr1 = new MinimumBoundingBox(minLon + offset * i, minLat + offset * j, minLon + offset * i + interval, minLat + offset * j + interval)
        val t = System.currentTimeMillis()
        val xzRanges: Seq[IndexRange] = sfc.range(minLon + offset * i, minLat + offset * j, minLon + offset * i + interval, minLat + offset * j + interval)
        val pLine: Pipeline = jedis.pipelined
        val pLineNC: Pipeline = jedis.pipelined
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
        val resultTmp = new java.util.HashMap[String, String](1000)
        for (o <- listNC.toArray()) {
          val value = o.asInstanceOf[util.LinkedHashSet[String]]
          querySize += value.size()
          for (v <- value.asScala) {
            val polygon = v.split("_")(0)
            WKTUtils.read(polygon)
            //size += 1
            if (mbr1.intersects(WKTUtils.read(polygon).getEnvelopeInternal)) {
              val ss = v.split("_")
              resultTmp.put(s"${ss(1)}", v)
              size += 1
            }
          }
        }
        val queryTime = System.currentTimeMillis() - t
        for (o <- list.toArray()) {
          val value = o.asInstanceOf[util.LinkedHashSet[String]]
          size += value.size()
          querySize += value.size()
          for (v <- value.asScala) {
            val ss = v.split("_")
            resultTmp.put(s"${ss(1)}", v)
          }
        }
        println(s"$queryTime,$querySize,${resultTmp.size()},${xzRanges.size}")
        pLineNC.close()
        pLine.close()
      }
    }
  }
}
