package curve

import curve.NativeSFC.{QueryWindow, XElement}
import org.locationtech.sfcurve.IndexRange
import java.util
import java.util.Comparator
import scala.collection.mutable.ArrayBuffer

class NativeSFC(g: Short, xBounds: (Double, Double), yBounds: (Double, Double)) extends XZ2SFC(g, xBounds, yBounds) {

  def code(bounds: (Double, Double, Double, Double)): util.ArrayList[Long] = code(bounds._1, bounds._2, bounds._3, bounds._4, false)

  def code(xmin: Double, ymin: Double, xmax: Double, ymax: Double, lenient: Boolean): java.util.ArrayList[Long] = {
    val (nxmin, nymin, nxmax, nymax) = normalize(xmin, ymin, xmax, ymax, lenient)
    encode(nxmin, nymin, nxmax, nymax)
  }

  private val LevelOneElements = XElement(0.0, 0.0, 1.0, 1.0, 1.0, g, 0.toShort, 0L).children
  // indicator that we have searched a full level of the quad/oct tree
  private val LevelTerminator = XElement(-1.0, -1.0, -1.0, -1.0, 0, g, 0.toShort, 0L);

  def encode(nxmin: Double, nymin: Double, nxmax: Double, nymax: Double): util.ArrayList[Long] = {
    var level: Short = 1
    val mrs = 500
    val maxStep = 10000
    val remaining = new java.util.ArrayDeque[XElement](100)
    LevelOneElements.foreach(remaining.add)
    remaining.add(LevelTerminator)
    val windows = QueryWindow(nxmin, nymin, nxmax, nymax)
    val ranges = new java.util.ArrayList[Long](10)
    var i = 0
    while (!remaining.isEmpty) {
      val next = remaining.poll
      i += 1
      if (next.eq(LevelTerminator)) {
        // we've fully processed a level, increment our state
        if (!remaining.isEmpty && level < g) {
          level = (level + 1).toShort
          remaining.add(LevelTerminator)
        }
      } else {
        if (null != next && next.overlaps(windows) && level < g) {
          next.children.foreach(remaining.add)
        }
      }
      if (level == g && next.overlaps(windows)) {
        ranges.add(next.code)
      }
    }
    ranges
  }

  def check(nxmin: Double, nymin: Double, nxmax: Double, nymax: Double, window: QueryWindow, level: Short): Unit = {
    if (level == g) {

    } else {
      val xCenter = (nxmin + nxmax) / 2
      val yCenter = (nymin + nymax) / 2
      if (intersect(nxmin, nymin, xCenter, yCenter, window)) {
        check(nxmin, nymin, xCenter, yCenter, window, (level + 1).toShort)
      }
      if (intersect(xCenter, nymin, nxmax, yCenter, window)) {
        check(xCenter, nymin, nxmax, yCenter, window, (level + 1).toShort)
      }
      if (intersect(xCenter, yCenter, nxmax, nymax, window)) {
        check(xCenter, yCenter, nxmax, nymax, window, (level + 1).toShort)
      }
      if (intersect(nxmin, yCenter, xCenter, nymax, window)) {
        check(nxmin, yCenter, xCenter, nymax, window, (level + 1).toShort)
      }
    }
  }

  def intersect(xmin: Double, ymin: Double, xmax: Double, ymax: Double, window: QueryWindow): Boolean = {
    if (window.xmax >= xmin && window.ymax >= ymin && window.xmin <= xmax && window.ymin <= ymax) {
      true
    } else {
      false
    }
  }

  def range(bounds: (Double, Double, Double, Double)): Seq[IndexRange] = {
    val codes = code(bounds._1, bounds._2, bounds._3, bounds._4, false)
    codes.sort(new Comparator[Long] {
      override def compare(o1: Long, o2: Long): Int = {
        java.lang.Long.compare(o1, o2)
      }
    })
    var current = IndexRange(codes.get(0), codes.get(0), false)
    val result = ArrayBuffer.empty[IndexRange]
    var i = 1
    while (i < codes.size()) {
      val value = codes.get(i)
      if (value <= current.upper + 1) {
        current = IndexRange(current.lower, math.max(current.upper, value), false)
      } else {
        result.append(current)
        current = IndexRange(value, value, false)
      }
      i += 1
    }
    result.append(current)
    result
  }

}

object NativeSFC {

  private val cache = new java.util.concurrent.ConcurrentHashMap[Short, NativeSFC]()

  def apply(g: Short): NativeSFC = {
    var sfc = cache.get(g)
    if (sfc == null) {
      sfc = new NativeSFC(g, (-180.0, 180.0), (-90.0, 90.0))
      cache.put(g, sfc)
    }
    sfc
  }

  /**
    * Region being queried. Bounds are normalized to [0-1].
    *
    * @param xmin x lower bound in [0-1]
    * @param ymin y lower bound in [0-1]
    * @param xmax x upper bound in [0-1], must be >= xmin
    * @param ymax y upper bound in [0-1], must be >= ymin
    */
  case class QueryWindow(xmin: Double, ymin: Double, xmax: Double, ymax: Double)

  /**
    * An extended Z curve element. Bounds refer to the non-extended z element for simplicity of calculation.
    *
    * An extended Z element refers to a normal Z curve element that has it's upper bounds expanded by double it's
    * width/height. By convention, an element is always square.
    *
    * @param xmin   x lower bound in [0-1]
    * @param ymin   y lower bound in [0-1]
    * @param xmax   x upper bound in [0-1], must be >= xmin
    * @param ymax   y upper bound in [0-1], must be >= ymin
    * @param length length of the non-extended side (note: by convention width should be equal to height)
    */
  private case class XElement(xmin: Double, ymin: Double, xmax: Double, ymax: Double, length: Double, g: Short, level: Short, code: Long) {

    // extended x and y bounds
    lazy val xext = xmax + length
    lazy val yext = ymax + length

    def isContained(window: QueryWindow): Boolean =
      window.xmin <= xmin && window.ymin <= ymin && window.xmax >= xmax && window.ymax >= ymax

    def overlaps(window: QueryWindow): Boolean =
      window.xmax >= xmin && window.ymax >= ymin && window.xmin <= xmax && window.ymin <= ymax

    def children: Seq[XElement] = {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      val len = length / 2.0
      val l = (level - 1.toShort).toShort
      val c = code
      val c0 = copy(xmax = xCenter, ymax = yCenter, length = len, level = l, code = c)
      val c1 = copy(xmin = xCenter, ymax = yCenter, length = len, level = l, code = c + math.pow(4, g - l).toLong)
      val c2 = copy(xmax = xCenter, ymin = yCenter, length = len, level = l, code = c + 2L * math.pow(4, g - l).toLong)
      val c3 = copy(xmin = xCenter, ymin = yCenter, length = len, level = l, code = c + 3L * math.pow(4, g - l).toLong)
      Seq(c0, c1, c2, c3)
    }
  }

}

