package curve

import curve.XZPlusSFC.{QueryWindow, XElement}
import org.locationtech.sfcurve.IndexRange

import scala.collection.mutable.ArrayBuffer
class XZPlusSFC(g: Short, xBounds: (Double, Double), yBounds: (Double, Double)) extends XZ2SFC(g, xBounds, yBounds) {


  override def index(xmin: Double, ymin: Double, xmax: Double, ymax: Double, lenient: Boolean = false): Long = {
    // normalize inputs to [0,1]
    val (nxmin, nymin, nxmax, nymax) = normalize(xmin, ymin, xmax, ymax, lenient)

    // calculate the length of the sequence code (section 4.1 of XZ-Ordering paper)

    val maxDim = math.max(nxmax - nxmin, nymax - nymin)

    // l1 (el-one) is a bit confusing to read, but corresponds with the paper's definitions
    val l1 = math.floor(math.log(maxDim) / XZSFC.LogPointFive).toInt

    // the length will either be (l1) or (l1 + 1)
    val length = if (l1 >= g) {
      g
    } else {
      val w2 = math.pow(0.5, l1 + 1) // width of an element at resolution l2 (l1 + 1)

      // predicate for checking how many axis the polygon intersects
      // math.floor(min / w2) * w2 == start of cell containing min
      def predicate(min: Double, max: Double): Boolean = max <= (math.floor(min / w2) * w2) + (2 * w2)

      if (predicate(nxmin, nxmax) && predicate(nymin, nymax)) l1 + 1 else l1
    }

    val w = math.pow(0.5, length)
    val x = math.floor(nxmin / w) * w
    val y = math.floor(nymin / w) * w

    var posCode = 1L

    if (nymax > y + w) {
      posCode = 3L
    }

    if (nymax > y + w && nxmax > x + w) {
      posCode = 2L
    }

    if (nymax < y + w && nxmax < x + w) {
      //println(length + "_4")
      posCode = 4L
    }
    //println(posCode)
    sequenceCode(nxmin, nymin, length, posCode)
  }

  def sequenceCode(x: Double, y: Double, length: Int, posCode: Long): Long = {
    var xmin = 0.0
    var ymin = 0.0
    var xmax = 1.0
    var ymax = 1.0

    var cs = 0L

    var i = 0
    while (i < length - 1) {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      (x < xCenter, y < yCenter) match {
        case (true, true) => cs += 3L; xmax = xCenter; ymax = yCenter
        case (false, true) => cs += 3L + 1L * (5 * math.pow(4, g - i - 1).toLong - 1); xmin = xCenter; ymax = yCenter
        case (true, false) => cs += 3L + 2L * (5 * math.pow(4, g - i - 1).toLong - 1); xmax = xCenter; ymin = yCenter
        case (false, false) => cs += 3L + 3L * (5 * math.pow(4, g - i - 1).toLong - 1); xmin = xCenter; ymin = yCenter
      }
      i += 1
    }

    if (i < length) {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      (x < xCenter, y < yCenter) match {
        case (true, true) => cs += posCode; xmax = xCenter; ymax = yCenter
        case (false, true) => cs += posCode + 1L * (5 * math.pow(4, g - i - 1).toLong - 1); xmin = xCenter; ymax = yCenter
        case (true, false) => cs += posCode + 2L * (5 * math.pow(4, g - i - 1).toLong - 1); xmax = xCenter; ymin = yCenter
        case (false, false) => cs += posCode + 3L * (5 * math.pow(4, g - i - 1).toLong - 1); xmin = xCenter; ymin = yCenter
      }
    }

    cs
  }

  def psc(quad: XElement, window: QueryWindow) = {
    var pos = (false, false, false)
    if (window.xmin < quad.xmax) {
      if (window.ymin < quad.ymax) {
        pos = (true, true, true)
      } else {
        pos = (false, true, true)
      }
    } else {
      if (window.ymin < quad.ymax) {
        pos = (true, true, false)
      } else {
        pos = (false, true, false)
      }
    }
    pos
  }

  def psc2(quad: XElement, window: QueryWindow) = {
    var pos = (false, false, false, false)
    if (window.xmin < quad.xmax) {
      if (window.ymin < quad.ymax) {
        pos = (true, true, true, true)
      } else {
        pos = (false, true, true, false)
      }
    } else {
      if (window.ymin < quad.ymax) {
        pos = (true, true, false, false)
      } else {
        pos = (false, true, false, false)
      }
    }
    pos
  }

  /**
    * Determine XZ-curve ranges that will cover a given query window
    *
    * @param query a window to cover in the form (xmin, ymin, xmax, ymax) where: all values are in user space
    * @return
    */
  override def ranges(query: (Double, Double, Double, Double)): Seq[IndexRange] = ranges(Seq(query))

  /**
    * Determine XZ-curve ranges that will cover a given query window
    *
    * @param query     a window to cover in the form (xmin, ymin, xmax, ymax) where all values are in user space
    * @param maxRanges a rough upper limit on the number of ranges to generate
    * @return
    */
  override def ranges(query: (Double, Double, Double, Double), maxRanges: Option[Int]): Seq[IndexRange] =
    ranges(Seq(query), maxRanges)

  /**
    * Determine XZ-curve ranges that will cover a given query window
    *
    * @param xmin min x value in user space
    * @param ymin min y value in user space
    * @param xmax max x value in user space, must be >= xmin
    * @param ymax max y value in user space, must be >= ymin
    * @return
    */
  override def ranges(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Seq[IndexRange] =
    ranges(Seq((xmin, ymin, xmax, ymax)))

  /**
    * Determine XZ-curve ranges that will cover a given query window
    *
    * @param xmin      min x value in user space
    * @param ymin      min y value in user space
    * @param xmax      max x value in user space, must be >= xmin
    * @param ymax      max y value in user space, must be >= ymin
    * @param maxRanges a rough upper limit on the number of ranges to generate
    * @return
    */
  override def ranges(xmin: Double, ymin: Double, xmax: Double, ymax: Double, maxRanges: Option[Int]): Seq[IndexRange] =
    ranges(Seq((xmin, ymin, xmax, ymax)), maxRanges)

  /**
    * Determine XZ-curve ranges that will cover a given query window
    *
    * @param queries   a sequence of OR'd windows to cover. Each window is in the form
    *                  (xmin, ymin, xmax, ymax) where all values are in user space
    * @param maxRanges a rough upper limit on the number of ranges to generate
    * @return
    */
  override def ranges(queries: Seq[(Double, Double, Double, Double)], maxRanges: Option[Int] = None): Seq[IndexRange] = {
    // normalize inputs to [0,1]
    val windows = queries.map { case (xmin, ymin, xmax, ymax) =>
      val (nxmin, nymin, nxmax, nymax) = normalize(xmin, ymin, xmax, ymax, lenient = false)
      QueryWindow(nxmin, nymin, nxmax, nymax)
    }
    ranges(windows.toArray, maxRanges.getOrElse(Int.MaxValue))
  }

  /**
    * Determine XZ-curve ranges that will cover a given query window
    *
    * @param query     a sequence of OR'd windows to cover, normalized to [0,1]
    * @param rangeStop a rough max value for the number of ranges to return
    * @return
    */
  def ranges(query: Array[QueryWindow], rangeStop: Int): Seq[IndexRange] = {
    import XZPlusSFC.{LevelOneElements, LevelTerminator}

    // stores our results - initial size of 100 in general saves us some re-allocation
    val ranges = new java.util.ArrayList[IndexRange](100)
    var rst = rangeStop * 2
    if (rst < 0) {
      rst = rangeStop
    }
    // values remaining to process - initial size of 100 in general saves us some re-allocation
    val remaining = new java.util.ArrayDeque[XElement](100)

    // checks if a quad is contained in the search space
    def isContained(quad: XElement): Boolean = {
      var i = 0
      while (i < query.length) {
        if (quad.isContained(query(i))) {
          return true
        }
        i += 1
      }
      false
    }

    // finding position code
    def posCode(quad: XElement) = {
      var i = 0
      val poscode = Array(false, false, false)
      while (i < query.length) {
        val ps = psc(quad, query(i))
        poscode(0) = poscode(0) || ps._1
        poscode(1) = poscode(1) || ps._2
        poscode(2) = poscode(2) || ps._3
        i += 1
      }
      //println(poscode)
      //      if (poscode(1)) {
      //        poscode(0) = true
      //        poscode(2) = true
      //      }
      //      poscode(1) = true
      //println(poscode(0) + "_" + poscode(1) + "_" + poscode(2))
      poscode
    }

    def posCode2(quad: XElement) = {
      var i = 0
      val poscode = Array(false, false, false, false)
      while (i < query.length) {
        val ps = psc2(quad, query(i))
        poscode(0) = poscode(0) || ps._1
        poscode(1) = poscode(1) || ps._2
        poscode(2) = poscode(2) || ps._3
        poscode(3) = poscode(3) || ps._4
        i += 1
      }
      //println(poscode)
      //      if (poscode(1)) {
      //        poscode(0) = true
      //        poscode(2) = true
      //      }
      //      poscode(1) = true
      //println(poscode(0) + "_" + poscode(1) + "_" + poscode(2))
      poscode
    }

    // checks if a quad overlaps the search space
    def isOverlapped(quad: XElement): Boolean = {
      var i = 0
      while (i < query.length) {
        if (quad.overlaps(query(i))) {
          return true
        }
        i += 1
      }
      false
    }

    // checks a single value and either:
    //   eliminates it as out of bounds
    //   adds it to our results as fully matching, or
    //   adds it to our results as partial matching and queues up it's children for further processing
    def checkValue(quad: XElement, level: Short): Unit = {
      if (isContained(quad)) {
        // whole range matches, happy day
        val (min, max) = sequenceInterval(quad.xmin, quad.ymin, level, 1L, partial = false)
        ranges.add(IndexRange(min, max, contained = true))
      } else if (isOverlapped(quad)) {
        // some portion of this range is excluded
        // add the partial match and queue up each sub-range for processing
        var ps = posCode(quad)
        if (level == g) {
          ps = posCode2(quad)
        }
        var i = 1L
        for (elem <- ps) {
          if (elem) {
            val (min, max) = sequenceInterval(quad.xmin, quad.ymin, level, i, partial = true)
            ranges.add(IndexRange(min, max, contained = false))
          }
          i += 1L
        }
        if(level < g) {
          quad.children.foreach(remaining.add)
        }
      }
    }

    // initial level
    LevelOneElements.foreach(remaining.add)
    remaining.add(LevelTerminator)

    // level of recursion
    var level: Short = 1
    while (!remaining.isEmpty ) {
      val next = remaining.poll
      if (next.eq(LevelTerminator)) {
        // we've fully processed a level, increment our state
        if (!remaining.isEmpty && level < g) {
          level = (level + 1).toShort
          remaining.add(LevelTerminator)
        }
      } else {
        checkValue(next, level)
      }
    }

//    while (level <= g && !remaining.isEmpty && ranges.size < rst) {
//      val next = remaining.poll
//      if (next.eq(LevelTerminator)) {
//        // we've fully processed a level, increment our state
//        if (!remaining.isEmpty) {
//          level = (level + 1).toShort
//          remaining.add(LevelTerminator)
//        }
//      } else {
//        checkValue(next, level)
//      }
//    }
//
//    // bottom out and get all the ranges that partially overlapped but we didn't fully process
//    while (!remaining.isEmpty) {
//      val quad = remaining.poll
//      if (quad.eq(LevelTerminator)) {
//        level = (level + 1).toShort
//      } else {
//        val (min, max) = sequenceInterval(quad.xmin, quad.ymin, level, 1L, partial = false)
//        ranges.add(IndexRange(min, max, contained = false))
//      }
//    }

    // we've got all our ranges - now reduce them down by merging overlapping values
    // note: we don't bother reducing the ranges as in the XZ paper, as accumulo handles lots of ranges fairly well
    ranges.sort(IndexRange.IndexRangeIsOrdered)

    var current = ranges.get(0) // note: should always be at least one range
    val result = ArrayBuffer.empty[IndexRange]
    var i = 1
    while (i < ranges.size()) {
      val range = ranges.get(i)
      if (range.lower <= current.upper + 1) {
        // merge the two ranges
        current = IndexRange(current.lower, math.max(current.upper, range.upper), current.contained && range.contained)
      } else {
        // append the last range and set the current range for future merging
        result.append(current)
        current = range
      }
      i += 1
    }
    // append the last range - there will always be one left that wasn't added
    result.append(current)
    //println("xzplus:" + level + "_" + ranges.size() + "_" + result.size)
    result
  }

  private def sequenceInterval(x: Double, y: Double, length: Short, psc: Long, partial: Boolean): (Long, Long) = {
    val min = sequenceCode(x, y, length, psc)
    // if a partial match, we just use the single sequence code as an interval
    // if a full match, we have to match all sequence codes starting with the single sequence code
    val max = if (partial) {
      min
    } else {
      // from lemma 3 in the XZ-Ordering paper
      //min - psc + 3L + (5 * math.pow(4, g - length).toLong - 1)
      min - psc + (5 * math.pow(4, g - length).toLong - 1)
    }
    (min, max)
  }
}

object XZPlusSFC {

  // the initial level of quads
  private val LevelOneElements = XElement(0.0, 0.0, 1.0, 1.0, 1.0).children

  // indicator that we have searched a full level of the quad/oct tree
  private val LevelTerminator = XElement(-1.0, -1.0, -1.0, -1.0, 0)

  private val cache = new java.util.concurrent.ConcurrentHashMap[Short, XZPlusSFC]()

  def apply(g: Short): XZPlusSFC = {
    var sfc = cache.get(g)
    if (sfc == null) {
      sfc = new XZPlusSFC(g, (-180.0, 180.0), (-90.0, 90.0))
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
  private case class QueryWindow(xmin: Double, ymin: Double, xmax: Double, ymax: Double)

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
  private case class XElement(xmin: Double, ymin: Double, xmax: Double, ymax: Double, length: Double) {

    // extended x and y bounds
    lazy val xext = xmax + length
    lazy val yext = ymax + length

    def isContained(window: QueryWindow): Boolean =
      window.xmin <= xmin && window.ymin <= ymin && window.xmax >= xext && window.ymax >= yext

    def overlaps(window: QueryWindow): Boolean =
      window.xmax >= xmin && window.ymax >= ymin && window.xmin <= xext && window.ymin <= yext

    def children: Seq[XElement] = {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      val len = length / 2.0
      val c0 = copy(xmax = xCenter, ymax = yCenter, length = len)
      val c1 = copy(xmin = xCenter, ymax = yCenter, length = len)
      val c2 = copy(xmax = xCenter, ymin = yCenter, length = len)
      val c3 = copy(xmin = xCenter, ymin = yCenter, length = len)
      Seq(c0, c1, c2, c3)
    }
  }

}