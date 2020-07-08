package client

import java.io.Closeable
import java.util

import curve.{WKTUtils, XZPlusSFC}
import random.MinimumBoundingBox
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.JavaConverters._


/**
 * @author : xxx
 * @description : The client to insert, update and delete non-point objects in Redis
 * @date : Created in 2020-06-08 10:38
 * @modified by :
 **/
class XZRClient(host: String, ip: Int, table: String, resolution: Int = 16) extends Closeable {
  private val split = "_"
  val jedis = new Jedis(host, ip, 10000)
  val pline = jedis.pipelined()

  def insert(id: String, geom: String, value: String): Unit = {
    val geometry = WKTUtils.read(geom)
    val bbox = geometry.getEnvelopeInternal
    val sfc = XZPlusSFC.apply(resolution.toShort)
    val index = sfc.index(bbox.getMinX, bbox.getMinY, bbox.getMaxX, bbox.getMaxY, false)
    jedis.zadd(table, index, String.format("%s%s%s%s%s", id, split, geom, split, value))
    //jedis.sync()
  }

  def rangeQuery(minLng: Double, minLat: Double, maxLng: Double, maxLat: Double): util.ArrayList[(String, String)] = {
    val queryWindow = new MinimumBoundingBox(minLng, minLat, maxLng, maxLat)
    val sfc = XZPlusSFC.apply(resolution.toShort)
    val pLineContains = jedis.pipelined
    val pLineIntersects: Pipeline = jedis.pipelined
    val xzRanges = sfc.ranges(minLng, minLat, maxLng, maxLat)
    for (elem <- xzRanges) {
      if (!elem.contained) {
        pLineIntersects.zrangeByScore(table, elem.lower, elem.upper);
      } else {
        pLineContains.zrangeByScore(table, elem.lower, elem.upper);
      }
    }
    val listIntersects = pLineIntersects.syncAndReturnAll()
    val listContains = pLineContains.syncAndReturnAll()
    val result = new util.ArrayList[(String, String)]()
    for (o <- listIntersects.toArray()) {
      val value = o.asInstanceOf[util.LinkedHashSet[String]]
      for (v <- value.asScala) {
        val polygon = v.split(split)(1)
        WKTUtils.read(polygon)
        //size += 1
        if (queryWindow.intersects(WKTUtils.read(polygon).getEnvelopeInternal)) {
          result.add((v.split(split)(0), v.split(split)(2)))
        }
      }
    }

    for (o <- listContains.toArray()) {
      val value = o.asInstanceOf[util.LinkedHashSet[String]]
      for (v <- value.asScala) {
        result.add((v.split(split)(0), v.split(split)(2)))
      }
    }
    result
  }

  def pipeline(id: String, geom: String, value: String): Unit = {
    val geometry = WKTUtils.read(geom)
    val bbox = geometry.getEnvelopeInternal
    val sfc = XZPlusSFC.apply(resolution.toShort)
    val index = sfc.index(bbox.getMinX, bbox.getMinY, bbox.getMaxX, bbox.getMaxY)
    pline.zadd(table, index, String.format("%s%s%s%s%s", id, split, geom, split, value))
  }

  def sync(): Unit = {
    pline.sync()
  }

  override def close(): Unit = {
    val jedis = new Jedis(host, ip, 10000)
    pline.close()
    jedis.close()
  }
}

object XZRClient {
  def apply(host: String, ip: Int, table: String, resolution: Int): XZRClient = {
    new XZRClient(host, ip, table, resolution)
  }

  def apply(host: String, ip: Int, table: String): XZRClient = {
    new XZRClient(host, ip, table)
  }
}