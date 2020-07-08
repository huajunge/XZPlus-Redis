package redis;

import curve.XZ2SFC;
import curve.XZPlusSFC;
import org.locationtech.sfcurve.IndexRange;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import scala.collection.Seq;

import java.util.LinkedHashSet;
import java.util.List;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2020-04-29 13:06
 * @modified by :
 **/
public class ReadRedis {
    public static void main(String[] args) {
        double minLon = 116.39172;
        double minLat = 39.80123;
        double maxLon = 116.40072;
        double maxLat = 39.81023;
        double interval = 0.01;
        XZPlusSFC xzPlusSFC = XZPlusSFC.apply((short) 16);
        XZ2SFC xz2SFC = XZ2SFC.apply((short) 16);

        Seq<IndexRange> xzpRanges = xzPlusSFC.ranges(minLon, minLat, maxLon, maxLat);
        Seq<IndexRange> xzRanges = xz2SFC.ranges(minLon, minLat, maxLon, maxLat);

        Jedis jedis = new Jedis("127.0.0.1", 6379);
        Pipeline pipelined = jedis.pipelined();
        pipelined.zrangeByScore("xzp_tdrive_test_hb", 4910563333L, 4910563333L);
        List<Object> o = pipelined.syncAndReturnAll();
        LinkedHashSet<Object> oo = (LinkedHashSet<Object>) o.get(0);
        System.out.println(oo.size());
        pipelined.close();
        jedis.close();
    }
}
