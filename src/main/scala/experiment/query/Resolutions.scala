package experiment.query

import java.util

import curve._
import org.locationtech.sfcurve.IndexRange
import random.MinimumBoundingBox
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.JavaConverters._
import scala.collection.Seq

object Resolutions {

  def main(args: Array[String]): Unit = {
    val table = args(0)
    val tablexz = args(1)
    val tableHB = args(2)
    val tableone = args(3)
    val tablenative = args(4)
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

    val jedis: Jedis = new Jedis(host, 6379, 5000)
    for (p <- sp to ep) {
      println(s"---precision:$p----")
      //      var minLon: Double = 116.34557
      //      var minLat: Double = 39.92818
      //      var maxLon: Double = 116.37057
      //      var maxLat: Double = 39.95318
      var minLon: Double = MBR(0).toDouble
      var minLat: Double = MBR(1).toDouble
      var maxLon: Double = MBR(2).toDouble
      var maxLat: Double = MBR(3).toDouble
      val interval: Double = 0.01
      var offset: Double = 0.01
      val xzPlusSFC: XZPlusSFC = XZPlusSFC.apply(p.toShort)
      val xz2SFC: XZ2SFC = XZ2SFC.apply(p.toShort)
      val zone: ZOneValueSFC = ZOneValueSFC.apply(p.toShort)
      val hb: HBPlusSFC = HBPlusSFC.apply(p.toShort)
      val native: NativeSFC = NativeSFC.apply(p.toShort)
      query(table, minLon, minLat, maxLon, maxLat, offset * 1, xzPlusSFC, jedis, false)
      query(table, minLon, minLat, maxLon, maxLat, offset * 1, xzPlusSFC, jedis, false)
      for (i <- 1 to 1) {
        println(s"$minLon, $minLat, $maxLon, $maxLat")
        //Thread.sleep(10);
        println(s"---xzp:$i----")
        query(s"${table}_$p", minLon, minLat, maxLon, maxLat, offset * i, xzPlusSFC, jedis)
        //Thread.sleep(10);
        println(s"---xz:$i----")
        query(s"${tablexz}_$p", minLon, minLat, maxLon, maxLat, offset * i, xz2SFC, jedis)
        //Thread.sleep(10);
        println(s"---hb:$i----")
        query(s"${tableHB}_$p", minLon, minLat, maxLon, maxLat, offset * i, hb, jedis)
        //Thread.sleep(10);
        println(s"---zone:$i----")
        query(s"${tableone}_$p", minLon, minLat, maxLon, maxLat, offset * i, zone, jedis)
        //Thread.sleep(10);
        println(s"---native:$i----")
        queryNative(s"${tablenative}_$p", minLon, minLat, maxLon, maxLat, offset * i, native, jedis)
        //      minLon = maxLon
        //      minLat = maxLat
        //      maxLon += i * interval
        //      maxLat += i * interval
        maxLon += interval
        maxLat += interval
        //offset += offset
      }
    }
    jedis.close()
  }

  private def query(table: String, minLon: Double, minLat: Double, maxLon: Double, maxLat: Double, offset: Double, sfc: XZ2SFC, jedis: Jedis, flag: Boolean = true) = {
    for (i <- -3 to 3) {
      for (j <- -3 to 3) {
        val mbr1 = new MinimumBoundingBox(minLon + offset * i, minLat + offset * j, maxLon + offset * i, maxLat + offset * j)
        val t = System.currentTimeMillis()
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
        if (flag) {
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

  def queryNative(table: String, minLon: Double, minLat: Double, maxLon: Double, maxLat: Double, offset: Double, sfc: NativeSFC, jedis: Jedis, log: Boolean = true) = {
    for (i <- -3 to 3) {
      for (j <- -3 to 3) {
        val mbr1 = new MinimumBoundingBox(minLon + offset * i, minLat + offset * j, maxLon + offset * i, maxLat + offset * j)
        val t = System.currentTimeMillis()

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
            curve.WKTUtils.read(polygon)
            //size += 1
            if (mbr1.intersects(curve.WKTUtils.read(polygon).getEnvelopeInternal)) {
              val ss = v.split("_")
              resultTmp.put(s"${ss(1)}", v)
              size += 1
            }
          }
        }
        for (o <- list.toArray()) {
          val value = o.asInstanceOf[util.LinkedHashSet[String]]
          size += value.size()
          querySize += value.size()
          for (v <- value.asScala) {
            val ss = v.split("_")
            resultTmp.put(s"${ss(1)}", v)
          }
        }
        val queryTime = System.currentTimeMillis() - t
        if (log) {
          println(s"$queryTime,$querySize,${resultTmp.size()},${xzRanges.size}")
        }
        pLineNC.close()
        pLine.close()
      }
    }
  }
}
