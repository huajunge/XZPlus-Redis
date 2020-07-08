package exemple;

import client.XZRClient;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * @author : xxx
 * @description :
 * @date : Created in 2020-06-08 16:38
 * @modified by :
 **/
public class RangeQuery {
    public static void main(String[] args) {
        XZRClient client = new XZRClient("127.0.0.1", 6379, "xzr_table2", 16);
        ArrayList<Tuple2<String, String>> result = client.rangeQuery(106.24618, 26.20497, 106.86618, 26.82497);
        System.out.println(result.size());
    }
}
