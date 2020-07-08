package random

import java.util

import com.urbancomputing.geomesa.model.basic.box.MinimumBoundingBox
import curve.{HBPlusSFC, NativeSFC, XZ2SFC, XZPlusSFC, ZOneValueSFC}
import org.locationtech.geomesa.utils.interop.WKTUtils
import org.locationtech.sfcurve.IndexRange
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.JavaConverters._
import scala.collection.Seq

object OtherAlgs {

  def main(args: Array[String]): Unit = {
    val tableXZP = args(0)
    val tableXZ = args(1)
    val tableHB = args(2)
    val tableZone = args(3)
    val tableNative = args(4)
    val sp = args(5).toShort
    val ep = args(6).toShort
    val host = args(7)
    val MBR = args(8).split(",")
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
      val xzp: XZPlusSFC = XZPlusSFC.apply(j.toShort)
      val xz: XZ2SFC = XZ2SFC.apply(j.toShort)
      val hb: HBPlusSFC = HBPlusSFC.apply(j.toShort)
      val zone: ZOneValueSFC = ZOneValueSFC.apply(j.toShort)
      val native: NativeSFC = NativeSFC.apply(j.toShort)

      println(s"---precision:$j----")
      query(tableZone + s"_$j", minLon, minLat, minLon + 0.001, minLat + 0.001, offset, zone, jedis, log = false)
      query(tableZone + s"_$j", minLon, minLat, minLon + 0.001, minLat + 0.001, offset, zone, jedis, log = false)
      println(s"---xzp:----")
      query(tableXZP + s"_$j", minLon, minLat, maxLon, maxLat, offset, xzp, jedis)
      println(s"---xz:----")
      query(tableXZ + s"_$j", minLon, minLat, maxLon, maxLat, offset, xz, jedis)
      println(s"---hb:----")
      query(tableHB + s"_$j", minLon, minLat, maxLon, maxLat, offset, hb, jedis)
      println(s"---zone:----")
      query(tableZone + s"_$j", minLon, minLat, maxLon, maxLat, offset, zone, jedis)
      println(s"---native:----")
      queryNative(tableNative + s"_$j", minLon, minLat, maxLon, maxLat, offset, native, jedis)
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
  }

  def queryNative(table: String, minLon: Double, minLat: Double, maxLon: Double, maxLat: Double, offset: Double, sfc: NativeSFC, jedis: Jedis, log: Boolean = true) = {
    for (i <- -1 to 1) {
      for (j <- -1 to 1) {
        val mbr1 = new MinimumBoundingBox(minLon + offset * i, minLat + offset * j, maxLon + offset * i, maxLat + offset * j)
        val xzRanges: Seq[IndexRange] = sfc.range(minLon + offset * i, minLat + offset * j, maxLon + offset * i, maxLat + offset * j)
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
              resultTmp.put(s"${ss(0)}_${ss(1)}_${ss(2)}", v)
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
            resultTmp.put(s"${ss(0)}_${ss(1)}_${ss(2)}", v)
          }
        }

        if (log) {
          println(s"$queryTime,$querySize,${resultTmp.size()},${xzRanges.size}")
        }
        pLineNC.close()
        pLine.close()
      }
    }
  }
}
