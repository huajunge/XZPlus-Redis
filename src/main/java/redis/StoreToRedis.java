package redis;

import com.urbancomputing.geomesa.model.basic.box.MinimumBoundingBox;
import com.urbancomputing.geomesa.model.basic.point.GpsPoint;
import com.urbancomputing.geomesa.model.basic.trajectory.Trajectory;
import com.urbancomputing.geomesa.process.preprocess.segmentation.StayPointDensitySegmenter;
import com.urbancomputing.geomesa.process.preprocess.segmentation.params.SegmenterParams;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import curve.HilbertSFC;
import curve.XZ2SFC;
import curve.XZPlusSFC;
import curve.ZOneValueSFC;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

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
public class StoreToRedis {
    public static DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    public static void main(String[] args) {
        String path = args[0];
        String table = args[1];
        String tablexz = args[2];
        String tableone = args[3];
        String tableHB = args[4];
        short precison = Short.parseShort(args[5]);
        boolean isLocal = Boolean.parseBoolean(args[6]);
        SparkConf sparkConf = new SparkConf().setAppName(StoreToRedis.class.getName());
        if (isLocal) {
            sparkConf.setMaster("local[*]");
        }
        SegmenterParams params = new SegmenterParams(30, 2000, 600, 1);
        StayPointDensitySegmenter segmenter = new StayPointDensitySegmenter(params);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<Trajectory> tRDD = sparkContext.wholeTextFiles(path, 100).flatMap(v -> {
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
        //System.out.println(tRDD.count());
        Jedis jds = new Jedis("127.0.0.1", 6379);
        jds.del(table);
        jds.del(tablexz);
        jds.del(tableone);
        jds.del(tableHB);
        //jds.close();
        tRDD.foreachPartition(trajectoryIterator -> {
            Jedis jedis = new Jedis("127.0.0.1", 6379);
            Pipeline pipelined = jedis.pipelined();
            XZPlusSFC xzPlusSFC = XZPlusSFC.apply(precison);
            XZ2SFC xz2SFC = XZ2SFC.apply(precison);
            ZOneValueSFC zoneSFC = ZOneValueSFC.apply(precison);
            HilbertSFC hilbertSFC = HilbertSFC.apply(precison);
            while (trajectoryIterator.hasNext()) {
                Trajectory tra = trajectoryIterator.next();
                MinimumBoundingBox mbr = tra.getTrajectoryFeatures().getMinimumBoundingBox();
                long xzp = xzPlusSFC.index(mbr.getMinX(), mbr.getMinY(), mbr.getMaxX(), mbr.getMaxY(), false);
                long xz = xz2SFC.index(mbr.getMinX(), mbr.getMinY(), mbr.getMaxX(), mbr.getMaxY(), false);
                long zone = zoneSFC.index(mbr.getMinX(), mbr.getMinY(), mbr.getMaxX(), mbr.getMaxY(), false);
                long hb = hilbertSFC.index(mbr.getMinX(), mbr.getMinY(), mbr.getMaxX(), mbr.getMaxY(), false);
                //System.out.println(xzp + ", xz:" + xz);
                pipelined.zadd(table, xzp, String.format("%s_%s", mbr.toPolygon(4326).toText(), tra.getTrajectoryID()));
                pipelined.zadd(tablexz, xz, String.format("%s_%s", mbr.toPolygon(4326).toText(), tra.getTrajectoryID()));
                pipelined.zadd(tableone, zone, String.format("%s_%s", mbr.toPolygon(4326).toText(), tra.getTrajectoryID()));
                pipelined.zadd(tableHB, hb, String.format("%s_%s", mbr.toPolygon(4326).toText(), tra.getTrajectoryID()));
            }
            pipelined.sync();
            pipelined.close();
            jedis.close();
        });
        sparkContext.close();
    }

    /**
     * 单个连接
     *
     * @param host
     * @param port
     * @return
     */
    public static Jedis cli_single(String host, int port) {
        try {
            return new Jedis(host, port);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 连接池
     *
     * @param host
     * @param port
     * @return
     */
    public static Jedis cli_pool(String host, int port) {
        JedisPoolConfig config = new JedisPoolConfig();
        // 最大连接数
        config.setMaxTotal(10);
        // 最大连接空闲数
        config.setMaxIdle(2);
        JedisPool jedisPool = new JedisPool(config, host, port);
        try {

            return jedisPool.getResource();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }
}