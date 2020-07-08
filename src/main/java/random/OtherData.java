package random;

import com.urbancomputing.geomesa.model.basic.box.MinimumBoundingBox;
import curve.NativeSFC;
import org.locationtech.geomesa.curve.ZOneValueSFC;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.Random;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2020-05-03 19:15
 * @modified by :
 **/
public class OtherData {
    public static void main(String[] args) throws InterruptedException {
        String[] mbr = "106.65618,26.61497,106.68118,26.63997".split(",");
        String host = "127.0.0.1";
        String tableZone = "precision1_zone";
        String tableNative = "precision1_native";
        double lat = 26.21497;
        double lon = 106.25618;
        Random random = new Random(1000000);
        Random randomLat = new Random(2661497);
        Jedis jedis = new Jedis(host, 6379, 10000);
        //XZPlusSFC xzPlusSFC = XZPlusSFC.apply((short) 16);
        Pipeline pipelined = jedis.pipelined();
        for (int i = 15; i <= 15; i++) {
            ZOneValueSFC zOneValueSFC = ZOneValueSFC.apply((short) i);
            NativeSFC nativeSFC = NativeSFC.apply((short) i);
            for (int k = 0; k < 100000; k++) {
                for (int j = 1; j <= 5; j++) {
                    double offset = random.nextDouble() * 0.5;
                    double offsetLat = randomLat.nextDouble() * 0.5;
                    //System.out.println(String.format("%s_%s", offset, m));
                    long xzp = zOneValueSFC.index(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005, false);
                    ArrayList<Object> xz = nativeSFC.code(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005, false);
                    MinimumBoundingBox mbr2 = new MinimumBoundingBox(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005);
                    //System.out.println(String.format("%s", mbr2.toPolygon(4326).toText()));
                    pipelined.zadd(String.format("%s_%d", tableZone, i), xzp, String.format("%s_%s_%s", mbr2.toPolygon(4326).toText(), i, j));
                    for (Object o : xz) {
                        pipelined.zadd(String.format("%s_%d", tableNative, i), (long) o, String.format("%s_%s_%s_%s", mbr2.toPolygon(4326).toText(), i, j, o));
                    }
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
