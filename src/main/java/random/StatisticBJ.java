package random;

import com.urbancomputing.geomesa.core.io.parse.basic.RoadSegmentParse;
import com.urbancomputing.geomesa.model.basic.box.MinimumBoundingBox;
import com.urbancomputing.geomesa.model.basic.road.RoadSegment;
import com.urbancomputing.geomesa.model.utils.GeoFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2020-06-05 11:51
 * @modified by :
 **/
public class StatisticBJ {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName(TdriveToRedisWithPrecision.class.getName());
        sparkConf.setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> tRDD = sparkContext.textFile("D:\\工作文档\\data\\bj\\Road_Network_BJ_2016Q1_recoding.txt", 500);
        Map<Integer, Long> count = tRDD.map(v -> {
            RoadSegment r = RoadSegmentParse.parse(v, ",", ";", false);
            //String[] value = stringIterator.next().split(";");
            MinimumBoundingBox mbr = r.getMbr();
            return (int) (dis(mbr) / 100.0);
        }).countByValue();

        for (Map.Entry<Integer, Long> value : count.entrySet()) {
            System.out.println(value.getKey() + "," + value.getValue());
        }
        sparkContext.close();
    }

    public static double dis(MinimumBoundingBox mbr) {
        double width = GeoFunction.getDistanceInM(mbr.getMinX(), mbr.getMinY(), mbr.getMaxX(), mbr.getMaxY());
        double height = GeoFunction.getDistanceInM(mbr.getMinX(), mbr.getMinY(), mbr.getMinX(), mbr.getMaxY());
        return Double.max(width, height);
    }
}
