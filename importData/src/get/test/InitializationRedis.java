package get.test;

import org.junit.Test;

import get.util.RedisUtil;
import redis.clients.jedis.Jedis;

public class InitializationRedis {
	@Test
	public void test01() {// 创建Redis连接实例
		RedisUtil redisUtil = new RedisUtil();
		Jedis jedis = redisUtil.getJedis();

		int[] baseId = {19,20,17,15,21,22,43,44,28,37,39,40,38,14,31,41,27,45,46};
		for(int i = 0; i < baseId.length; i++){
			String id = String.valueOf(baseId[i]);
			//jedis.set("31", 0 + "");
			jedis.set(id, 0 + "");
			int j = i + 1;
			System.out.println("第" + j + "个库:" + "\"DI:"+ id + "\";记录数:" +jedis.get(id));
		}
		
		redisUtil.returnResource(jedis);

	}

}
