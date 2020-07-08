package redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.List;

/**
 * @author : hehuajun3
 * @description : test
 * @date : Created in 2020-04-30 15:01
 * @modified by :
 **/
public class TestRedis {
    public static void main(String[] args) {
        Jedis jds = new Jedis("114.67.225.97", 6379);
        Pipeline pipelined = jds.pipelined();
//        jds.zadd("test_tt", 1, "1");
//        jds.zadd("test_tt", 2, "2");
//        jds.zadd("test_tt", 3, "3");
//        jds.zadd("test_tt", 4, "4");
//        jds.zadd("test_tt", 5, "6");
//        pipelined.zadd("test_tt", 11, "11");
//        pipelined.zadd("test_tt", 12, "12");
//        pipelined.zadd("test_tt", 13, "13");
//        pipelined.zadd("test_tt", 14, "14");
//        pipelined.zadd("test_tt", 15, "16");
//        pipelined.sync();

        pipelined.zrangeByScore("test_tt", 1, 5);
        List<Object> objectList = pipelined.syncAndReturnAll();
        pipelined.close();
        jds.close();
    }
}
