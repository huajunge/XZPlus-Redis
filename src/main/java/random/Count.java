package random;

import com.urbancomputing.geomesa.model.basic.box.MinimumBoundingBox;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.geomesa.curve.XZ2SFC;
import org.locationtech.geomesa.curve.XZPlusSFC;
import org.locationtech.geomesa.utils.interop.WKTUtils;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2020-05-27 19:55
 * @modified by :
 **/
public class Count {
    public static void main(String[] args) {
        String path = args[0];
        String table = args[1];
        String tablexz = args[2];
        //String tableHB = args[3];
        short sp = Short.parseShort(args[4]);
        short ep = Short.parseShort(args[5]);
        String host = args[6];
        boolean isLocal = Boolean.parseBoolean(args[7]);
        SparkConf sparkConf = new SparkConf().setAppName(TdriveToRedisWithPrecision.class.getName());
        if (isLocal) {
            sparkConf.setMaster("local[*]");
        }
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> tRDD = sparkContext.textFile("D:\\工作文档\\data\\us_ploys\\us_ploys.csv", 500);
        //System.out.println("--------------");
        //System.out.println(tRDD.count());
        MinimumBoundingBox minimumBoundingBox = new MinimumBoundingBox(-77.5937, 40.871, -73.618, 43.2243);
        tRDD.foreachPartition(stringIterator -> {
            Jedis jedis = new Jedis(host, 6379, 10000);
            Pipeline pipelined = jedis.pipelined();
            while (stringIterator.hasNext()) {
                String[] value = stringIterator.next().split(";");
                Geometry geometry = WKTUtils.read(value[1]);
                Envelope envelope = geometry.getEnvelopeInternal();
                if (envelope.intersects(minimumBoundingBox)) {
                    for (Short i = sp; i <= ep; i++) {
                        XZPlusSFC xzPlusSFC = XZPlusSFC.apply(i);
                        XZ2SFC xz2SFC = XZ2SFC.apply(i);
                        long xzp = xzPlusSFC.index(envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY(), false);
                        long xz = xz2SFC.index(envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY(), false);
                        //System.out.println(xzp + ", xz:" + xz);
                        pipelined.zadd(String.format("%s_%d", table, i), xzp, String.format("%s_%s", geometry.toText(), value[0]));
                        pipelined.zadd(String.format("%s_%d", tablexz, i), xz, String.format("%s_%s", geometry.toText(), value[0]));
                    }
                }
            }
            pipelined.sync();
            pipelined.close();
            jedis.close();
        });
        sparkContext.close();
    }
}
