package random;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

/**
 * @author : suiyuan
 * @description : 大地坐标点（用经纬度来表示空间位置）
 * @date : Created in 2019-04-03 12:33
 * @modified by :
 **/
public class GeodeticPoint extends Point {

    /**
     * 默认大地坐标系为WGS84
     */
    private static final int SRID = 4326;

    /**
     * @param coordinates 坐标序列
     * @param factory     几何图形工厂
     */
    public GeodeticPoint(CoordinateSequence coordinates, GeometryFactory factory) {
        super(coordinates, factory);
    }

    /**
     * @param coordinate 点坐标
     **/
    public GeodeticPoint(Coordinate coordinate) {
        super(new CoordinateArraySequence(new Coordinate[]{coordinate}), new GeometryFactory(new PrecisionModel(), SRID));
    }

    /**
     * @param lng 经度
     * @param lat 纬度
     */
    public GeodeticPoint(double lng, double lat) {
        super(new CoordinateArraySequence(new Coordinate[]{new Coordinate(lng, lat, 0.0)}), new GeometryFactory(new PrecisionModel(), SRID));
    }

    /**
     * 设置大地坐标系
     *
     * @param srid 大地坐标系
     */
    public void setGEOGCS(int srid) {
        super.setSRID(srid);
    }


    /**
     * 获取大地坐标系
     *
     * @return
     */
    public int getGEOGCS() {
        return super.getSRID();
    }

    /**
     * 获取大地坐标点的经度
     *
     * @return java.lang.Double
     */
    public double getLng() {
        return getX();
    }

    /**
     * 设置大地坐标点的经度
     *
     * @param lng 经度
     */
    public void setLng(double lng) {
        this.getCoordinate().setX(lng);
    }

    /**
     * 获取大地坐标点的纬度
     *
     * @return java.lang.Double 当前大地坐标点的纬度
     */
    public double getLat() {
        return getY();
    }

    /**
     * 设置大地坐标点的纬度
     *
     * @param lat 纬度
     */
    public void setLat(double lat) {
        this.getCoordinate().setY(lat);
    }

    /**
     * 获取大地坐标点的海拔高度
     *
     * @return java.lang.Double 海波高度
     */
    public double getAlt() {
        return this.getCoordinate().getZ();
    }

    /**
     * 设置大地坐标点的海拔高度
     *
     * @param alt 海拔高度
     */
    public void setAlt(double alt) {
        this.getCoordinate().setZ(alt);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public boolean equals(Geometry g) {
        return super.equals(g);
    }

    @Override
    public String toString() {
        return "GeodeticPoint [lng=" + getLng() + ", lat=" + getLat() + ", alt=" + getAlt() + "]";
    }
}
