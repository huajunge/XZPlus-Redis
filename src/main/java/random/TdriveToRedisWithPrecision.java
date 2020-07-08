package random;

import com.urbancomputing.geomesa.model.basic.point.GpsPoint;
import com.urbancomputing.geomesa.model.basic.trajectory.Trajectory;
import com.urbancomputing.geomesa.process.preprocess.segmentation.StayPointDensitySegmenter;
import com.urbancomputing.geomesa.process.preprocess.segmentation.params.SegmenterParams;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2020-04-29 11:21
 * @modified by :
 **/
public class TdriveToRedisWithPrecision {
    public static DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    public static void main(String[] args) {
        String path = args[0];
        String table = args[1];
        String tablexz = args[2];
        String tableHB = args[3];
        short sp = Short.parseShort(args[4]);
        short ep = Short.parseShort(args[5]);
        String host = args[6];
        boolean isLocal = Boolean.parseBoolean(args[7]);
        SparkConf sparkConf = new SparkConf().setAppName(TdriveToRedisWithPrecision.class.getName());
        sparkConf.setMaster("local[*]");
        SegmenterParams params = new SegmenterParams(10, 500, 600, 0.5);
        StayPointDensitySegmenter segmenter = new StayPointDensitySegmenter(params);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<Trajectory> tRDD = sparkContext.wholeTextFiles(path, 500).flatMap(v -> {
            String id = v._1;
            if (null == v._2 || null == id) {
                return null;
            }
            int sIndex = id.lastIndexOf("/");
            int eIndex = id.lastIndexOf(".");
            id = id.substring(sIndex + 1, eIndex);
            String[] pointStrs = v._2.split("\\n");
            List<GpsPoint> pointList = new ArrayList<>(pointStrs.length);
            for (String pointStr : pointStrs) {
                String[] values = pointStr.split(",");
                try {
                    String pid = values[0];
                    ZonedDateTime time = ZonedDateTime.parse(values[1], timeFormatter);
                    double lat = Double.parseDouble(values[3]);
                    double lon = Double.parseDouble(values[2]);
                    if (lon < 180 && lon > -180 && lat < 90 && lat > -90) {
                        pointList.add(new GpsPoint(pid, lon, lat, time));
                    }
                } catch (Exception e) {

                }
            }
            if (!pointList.isEmpty()) {
                Trajectory trajectory = new Trajectory(id, id, pointList);
                trajectory.getTrajectoryFeatures();
                return segmenter.segment(trajectory).iterator();
            }
            return null;
        }).filter(Objects::nonNull);
        System.out.println(tRDD.count());
//        Jedis jds = new Jedis("127.0.0.1", 6379);
//        jds.del(table);
//        jds.del(tablexz);
//        jds.del(tableone);
//        jds.del(tableHB);
        //jds.close();
//        tRDD.foreachPartition(trajectoryIterator -> {
//            Jedis jedis = new Jedis(host, 6379, 10000);
//            Pipeline pipelined = jedis.pipelined();
//            while (trajectoryIterator.hasNext()) {
//                Trajectory tra = trajectoryIterator.next();
//                MinimumBoundingBox mbr = tra.getTrajectoryFeatures().getMinimumBoundingBox();
//                for (Short i = sp; i <= ep; i++) {
//                    XZPlusSFC xzPlusSFC = XZPlusSFC.apply(i);
//                    XZ2SFC xz2SFC = XZ2SFC.apply(i);
//                    HBPlusSFC hilbertSFC = HBPlusSFC.apply(i);
//                    long xzp = xzPlusSFC.index(mbr.getMinX(), mbr.getMinY(), mbr.getMaxX(), mbr.getMaxY(), false);
//                    long xz = xz2SFC.index(mbr.getMinX(), mbr.getMinY(), mbr.getMaxX(), mbr.getMaxY(), false);
//                    long hb = hilbertSFC.index(mbr.getMinX(), mbr.getMinY(), mbr.getMaxX(), mbr.getMaxY(), false);
//                    //System.out.println(xzp + ", xz:" + xz);
//                    pipelined.zadd(String.format("%s_%d", table, i), xzp, String.format("%s_%s", mbr.toPolygon(4326).toText(), tra.getTrajectoryID()));
//                    pipelined.zadd(String.format("%s_%d", tablexz, i), xz, String.format("%s_%s", mbr.toPolygon(4326).toText(), tra.getTrajectoryID()));
//                    pipelined.zadd(String.format("%s_%d", tableHB, i), hb, String.format("%s_%s", mbr.toPolygon(4326).toText(), tra.getTrajectoryID()));
//                }
//            }
//            pipelined.sync();
//            pipelined.close();
//            jedis.close();
//        });
        sparkContext.close();
    }
}