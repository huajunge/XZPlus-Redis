package exemple;

import client.XZRClient;
import random.MinimumBoundingBox;

import java.util.Random;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2020-06-08 14:51
 * @modified by :
 **/
public class InsertExample {
    public static void main(String[] args) {
        XZRClient client = new XZRClient("127.0.0.1", 6379, "xzr_table2", 16);
        double lat = 26.21497;
        double lon = 106.25618;
        Random random = new Random(1000000);
        Random randomLat = new Random(2661497);
        for (int j = 1; j <= 5; j++) {
            for (int i = 1; i <= 100; i++) {
                double offset = random.nextDouble() * 0.5;
                double offsetLat = randomLat.nextDouble() * 0.5;
                MinimumBoundingBox mbr = new MinimumBoundingBox(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005);
                client.insert((j - 1) * 100 + i + "", mbr.toPolygon(4326).toText(), mbr.toPolygon(4326).toText());
            }
        }
        client.close();
    }
}
