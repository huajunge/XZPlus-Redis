package random;

import com.urbancomputing.geomesa.model.basic.box.MinimumBoundingBox;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.geomesa.utils.interop.WKTUtils;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2020-06-02 11:27
 * @modified by :
 **/
public class Statistic {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName(TdriveToRedisWithPrecision.class.getName());
        sparkConf.setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> tRDD = sparkContext.textFile("D:\\工作文档\\data\\us_ploys\\us_ploys.csv", 1000);
        MinimumBoundingBox minimumBoundingBox = new MinimumBoundingBox(-77.5937, 40.871, -73.618, 43.2243);
        JavaRDD<String> t = tRDD.filter(v1 -> {
            String[] value = v1.split(";");
            Geometry geometry = WKTUtils.read(value[1]);
            Envelope envelope = geometry.getEnvelopeInternal();
            return envelope.intersects(minimumBoundingBox);
        });
        System.out.println("--------------------\n"+t.count());
    }
}