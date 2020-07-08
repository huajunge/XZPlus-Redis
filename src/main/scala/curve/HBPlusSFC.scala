package curve

class HBPlusSFC(g: Short, xBounds: (Double, Double), yBounds: (Double, Double)) extends XZPlusSFC(g, xBounds, yBounds) {
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
      //println(g + "_4")
      posCode = 4L
    }
    //println(posCode)
    sequenceCode(nxmin, nymin, length, posCode)
  }

  val code = Array(
    Array(0L, 1L, 2L, 3L),
    Array(0L, 3L, 2L, 1L),
    Array(2L, 1L, 0L, 3L),
    Array(2L, 3L, 0L, 1L))
  val dir = Array(
    Array(1, 0, 0, 2),
    Array(0, 3, 1, 1),
    Array(2, 2, 3, 0),
    Array(3, 1, 2, 3))

  def offset(current: Int, quad: Int): (Long, Int) = {
    (code(current)(quad), dir(current)(quad))
  }

  override def sequenceCode(x: Double, y: Double, length: Int, posCode: Long): Long = {
    var xmin = 0.0
    var ymin = 0.0
    var xmax = 1.0
    var ymax = 1.0

    var cs = 0L

    var i = 0
    var currentDir = 0
    var of = offset(currentDir, 0)
    while (i < length - 1) {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      (x < xCenter, y < yCenter) match {
        case (true, true) =>
          of = offset(currentDir, 0)
          xmax = xCenter;
          ymax = yCenter
        case (true, false) =>
          of = offset(currentDir, 1)
          xmax = xCenter;
          ymin = yCenter
        case (false, true) =>
          of = offset(currentDir, 2)
          xmin = xCenter;
          ymax = yCenter
        case (false, false) =>
          of = offset(currentDir, 3)
          xmin = xCenter;
          ymin = yCenter
      }
      cs += 3L + of._1 * (5 * math.pow(4, g - i - 1).toLong - 1)
      currentDir = of._2
      i += 1
    }
    if (i < length) {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      (x < xCenter, y < yCenter) match {
        case (true, true) =>
          of = offset(currentDir, 0)
          xmax = xCenter;
          ymax = yCenter
        case (true, false) =>
          of = offset(currentDir, 1)
          xmax = xCenter;
          ymin = yCenter
        case (false, true) =>
          of = offset(currentDir, 2)
          xmin = xCenter;
          ymax = yCenter
        case (false, false) =>
          of = offset(currentDir, 3)
          xmin = xCenter;
          ymin = yCenter
      }
      cs += posCode + of._1 * (5 * math.pow(4, g - i - 1).toLong - 1)
    }
    cs
  }
}

object HBPlusSFC {
  private val cache = new java.util.concurrent.ConcurrentHashMap[Short, HBPlusSFC]()

  def apply(g: Short): HBPlusSFC = {
    var sfc = cache.get(g)
    if (sfc == null) {
      sfc = new HBPlusSFC(g, (-180.0, 180.0), (-90.0, 90.0))
      cache.put(g, sfc)
    }
    sfc
  }
}

