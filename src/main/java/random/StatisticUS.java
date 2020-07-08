package random;

import com.urbancomputing.geomesa.model.basic.box.MinimumBoundingBox;
import com.urbancomputing.geomesa.model.utils.GeoFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.geomesa.utils.interop.WKTUtils;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.util.Map;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2020-06-05 11:30
 * @modified by :
 **/
public class StatisticUS {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName(TdriveToRedisWithPrecision.class.getName());
        sparkConf.setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> tRDD = sparkContext.textFile("D:\\工作文档\\data\\us_ploys\\us_ploys.csv", 500);
        //System.out.println("--------------");
        //System.out.println(tRDD.count());
        MinimumBoundingBox minimumBoundingBox = new MinimumBoundingBox(-77.5937, 40.871, -73.618, 43.2243);
        Map<Integer, Long> count = tRDD.map(v -> {
            String[] value = v.split(";");
            Geometry geometry = WKTUtils.read(value[1]);
            Envelope envelope = geometry.getEnvelopeInternal();
            return (int) (dis(envelope) / 100.0);
        }).countByValue();

        for (Map.Entry<Integer, Long> value : count.entrySet()) {
            System.out.println(value.getKey() + "," + value.getValue());
        }
        sparkContext.close();
    }

    public static double dis(Envelope envelope) {
        double width = GeoFunction.getDistanceInM(envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMinY());
        double height = GeoFunction.getDistanceInM(envelope.getMinX(), envelope.getMinY(), envelope.getMinX(), envelope.getMaxY());
        return Double.max(width, height);
    }
}
