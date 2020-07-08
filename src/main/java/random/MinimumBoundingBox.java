package random;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2020-07-08 10:49
 * @modified by :
 **/
public class MinimumBoundingBox extends Envelope {
    /**
     * WGS84
     */
    private static final int SRID = 4326;

    /**
     * centerPoint
     * centerPoint
     */
    private GeodeticPoint centerPoint;

    /**
     * MinimumBoundingBox
     *
     * @param lowerLeft
     * @param upperRight
     */
    public MinimumBoundingBox(GeodeticPoint lowerLeft, GeodeticPoint upperRight) {
        super(lowerLeft.getLng(), upperRight.getLng(), lowerLeft.getLat(), upperRight.getLat());
    }

    /**
     * MinimumBoundingBox
     *
     * @param lng1
     * @param lat1
     * @param lng2
     * @param lat2
     */
    public MinimumBoundingBox(double lng1, double lat1, double lng2, double lat2) {
        super(lng1, lng2, lat1, lat2);
    }

    /**
     * getLowerLeft
     */
    public GeodeticPoint getLowerLeft() {
        return new GeodeticPoint(getMinX(), getMinY());
    }

    /**
     * getUpperRight
     */
    public GeodeticPoint getUpperRight() {
        return new GeodeticPoint(getMaxX(), getMaxY());
    }

    /**
     * @return double
     */
    public double getMinLat() {
        return getMinY();
    }

    /**
     * @return double
     */
    public double getMinLng() {
        return getMinX();
    }

    /**
     * @return double
     */
    public double getMaxLat() {
        return getMaxY();
    }

    /**
     * @return double
     */
    public double getMaxLng() {
        return getMaxX();
    }

    /**
     * @return GeodeticPoint
     */
    public GeodeticPoint getCenterPoint() {
        if (null == this.centerPoint) {
            Coordinate coordinate = this.centre();
            this.centerPoint = new GeodeticPoint(coordinate.getX(), coordinate.getY());
        }
        return this.centerPoint;
    }

    /**
     * isIntersects
     *
     * @param mbr
     * @return boolean true:Intersect，false:Not Intersect
     */
    public boolean isIntersects(MinimumBoundingBox mbr) {
        if (null == mbr) {
            return false;
        }
        return super.intersects(mbr);
    }

    public MinimumBoundingBox intersects(MinimumBoundingBox mbr) {
        if (!isIntersects(mbr)) {
            return null;
        }
        //TODO java.lang.ClassCastException
        //return (MinimumBoundingBox) intersection(mbr);
        Envelope envelope = intersection(mbr);
        return new MinimumBoundingBox(envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY());
    }

    public MinimumBoundingBox union(MinimumBoundingBox mbr) {
        if (null == mbr) {
            return this;
        }
        double minLat = Math.min(getMinLat(), mbr.getMinLat());
        double maxLat = Math.max(getMaxLat(), mbr.getMaxLat());
        double minLng = Math.min(getMinLng(), mbr.getMinLng());
        double maxLng = Math.max(getMaxLng(), mbr.getMaxLng());
        return new MinimumBoundingBox(new GeodeticPoint(minLng, minLat), new GeodeticPoint(maxLng, maxLat));
    }

    @Override
    public String toString() {
        return this.toPolygon(SRID).toString();
    }

    public Polygon toPolygon(int srid) {
        GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
        CoordinateSequence cs = new CoordinateArraySequence(new Coordinate[]{
                new Coordinate(getMinLng(), getMaxLat()),
                new Coordinate(getMaxLng(), getMaxLat()),
                new Coordinate(getMaxLng(), getMinLat()),
                new Coordinate(getMinLng(), getMinLat()),
                new Coordinate(getMinLng(), getMaxLat())
        });
        LinearRing shell = new LinearRing(cs, geometryFactory);
        return geometryFactory.createPolygon(shell);
    }
}

