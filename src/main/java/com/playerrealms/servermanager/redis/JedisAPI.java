package com.playerrealms.servermanager.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

public class JedisAPI {

	private static JedisPool pool;
	
	public static void init(String host, int port, String password) {
		if(pool != null) {
			throw new IllegalArgumentException("Already initialized.");
		}
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(200);
		if(password.isEmpty()) {
			pool = new JedisPool(config, host, port, 200);
		}else {
			pool = new JedisPool(config, host, port, 200, password);
		}

	}
	
	/**
	 * Blocks the current thread and subscribes
	 * @param sub
	 * @param channels
	 */
	public static void subscribe(JedisPubSub sub, String...channels) {
		try(Jedis jedis = pool.getResource()){
			jedis.subscribe(sub, channels);
		}
	}
	
	public static void publish(String channel, String msg) {
		try(Jedis jedis = pool.getResource()){
			jedis.publish(channel, msg);
		}
	}
	
}
