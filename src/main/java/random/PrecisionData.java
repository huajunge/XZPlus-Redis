package random;

import com.urbancomputing.geomesa.model.basic.box.MinimumBoundingBox;
import curve.HBPlusSFC;
import curve.XZ2SFC;
import curve.XZPlusSFC;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Random;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2020-05-03 19:15
 * @modified by :
 **/
public class PrecisionData {
    public static void main(String[] args) throws InterruptedException {
        String[] mbr = "106.65618,26.61497,106.68118,26.63997".split(",");
        String host = "127.0.0.1";
        String table = "precision1_xzp";
        String tablexz = "precision1_xz2";
        String tableHB = "precision1_hbp";
        double lat = 26.21497;
        double lon = 106.25618;
        Random random = new Random(1000000);
        Random randomLat = new Random(2661497);
        Jedis jedis = new Jedis(host, 6379, 10000);
        //XZPlusSFC xzPlusSFC = XZPlusSFC.apply((short) 16);
        Pipeline pipelined = jedis.pipelined();
        for (int i = 17; i <= 18; i++) {
            XZPlusSFC xzPlusSFC = XZPlusSFC.apply((short) i);
            XZ2SFC xz2SFC = XZ2SFC.apply((short) i);
            HBPlusSFC hilbertSFC = HBPlusSFC.apply((short) i);
            for (int k = 0; k < 100000; k++) {
                for (int j = 1; j <= 5; j++) {
                    double offset = random.nextDouble() * 0.5;
                    double offsetLat = randomLat.nextDouble() * 0.5;
                    //System.out.println(String.format("%s_%s", offset, m));
                    long xzp = xzPlusSFC.index(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005, false);
                    long xz = xz2SFC.index(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005, false);
                    long hb = hilbertSFC.index(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005, false);
                    MinimumBoundingBox mbr2 = new MinimumBoundingBox(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005);
                    //System.out.println(String.format("%s", mbr2.toPolygon(4326).toText()));
                    pipelined.zadd(String.format("%s_%d", table, i), xzp, String.format("%s_%s_%s", mbr2.toPolygon(4326).toText(), i, j));
                    pipelined.zadd(String.format("%s_%d", tablexz, i), xz, String.format("%s_%s_%s", mbr2.toPolygon(4326).toText(), i, j));
                    pipelined.zadd(String.format("%s_%d", tableHB, i), hb, String.format("%s_%s_%s", mbr2.toPolygon(4326).toText(), i, j));
                }
            }
            pipelined.sync();
            pipelined.close();
            System.out.println("store ----" + i);
            //sleep(1000);
            pipelined = jedis.pipelined();
        }
        //pipelined.sync();
        //pipelined.close();
        jedis.close();
    }
}
