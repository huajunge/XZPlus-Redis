package random;

import com.urbancomputing.geomesa.core.io.parse.basic.RoadSegmentParse;
import com.urbancomputing.geomesa.model.basic.box.MinimumBoundingBox;
import com.urbancomputing.geomesa.model.basic.road.RoadSegment;
import curve.*;
import curve.XZ2SFC;
import curve.XZPlusSFC;
import curve.ZOneValueSFC;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2020-06-02 18:39
 * @modified by :
 **/
public class Road {
    public static void main(String[] args) {
        String path = args[0];
        String table = args[1];
        String tablexz = args[2];
        String tableHB = args[3];
        String tablezone = args[4];
        String tableNative = args[5];
        //String tableHB = args[3];
        short sp = Short.parseShort(args[6]);
        short ep = Short.parseShort(args[7]);
        String host = args[8];
        boolean isLocal = Boolean.parseBoolean(args[9]);
        SparkConf sparkConf = new SparkConf().setAppName(TdriveToRedisWithPrecision.class.getName());
        if (isLocal) {
            sparkConf.setMaster("local[*]");
        }
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> tRDD = sparkContext.textFile("D:\\工作文档\\data\\bj\\Road_Network_BJ_2016Q1_recoding.txt", 500);
        tRDD.foreachPartition(stringIterator -> {
            Jedis jedis = new Jedis(host, 6379, 10000);
            Pipeline pipelined = jedis.pipelined();
            while (stringIterator.hasNext()) {
                RoadSegment r = RoadSegmentParse.parse(stringIterator.next(), ",", ";", false);
                //String[] value = stringIterator.next().split(";");
                MinimumBoundingBox mbr = r.getMbr();
                for (Short i = sp; i <= ep; i++) {
                    XZPlusSFC xzPlusSFC = XZPlusSFC.apply(i);
                    XZ2SFC xz2SFC = XZ2SFC.apply(i);
                    HBPlusSFC hbPlusSFC = HBPlusSFC.apply(i);
                    ZOneValueSFC zOneValueSFC = ZOneValueSFC.apply(i);

                    long xzp = xzPlusSFC.index(mbr.getMinX(), mbr.getMinY(), mbr.getMaxX(), mbr.getMaxY(), false);
                    long xz = xz2SFC.index(mbr.getMinX(), mbr.getMinY(), mbr.getMaxX(), mbr.getMaxY(), false);
                    long hb = hbPlusSFC.index(mbr.getMinX(), mbr.getMinY(), mbr.getMaxX(), mbr.getMaxY(), false);
                    long zone = zOneValueSFC.index(mbr.getMinX(), mbr.getMinY(), mbr.getMaxX(), mbr.getMaxY(), false);
                    if (!(i == 15 || i == 16)) {
                        pipelined.zadd(String.format("%s_%d", table, i), xzp, String.format("%s_%s", mbr.toPolygon(4326).toText(), r.getSegmentId()));
                        pipelined.zadd(String.format("%s_%d", tablexz, i), xz, String.format("%s_%s", mbr.toPolygon(4326).toText(), r.getSegmentId()));
                    }
                    //System.out.println(xzp + ", xz:" + xz);
                    pipelined.zadd(String.format("%s_%d", tableHB, i), hb, String.format("%s_%s", mbr.toPolygon(4326).toText(), r.getSegmentId()));
                    pipelined.zadd(String.format("%s_%d", tablezone, i), zone, String.format("%s_%s", mbr.toPolygon(4326).toText(), r.getSegmentId()));
                    NativeSFC nativeSFC = NativeSFC.apply(i);
                    ArrayList<Object> codes = nativeSFC.code(mbr.getMinX(), mbr.getMinY(), mbr.getMaxX(), mbr.getMaxY(), false);
                    for (Object code : codes) {
                        pipelined.zadd(String.format("%s_%d", tableNative, i), (Long) code, String.format("%s_%s_%s", mbr.toPolygon(4326).toText(), r.getSegmentId(), code));
                    }
                }
            }
            pipelined.sync();
            pipelined.close();
            jedis.close();
        });
    }
}
