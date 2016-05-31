package com.zqh.nosql.redis.springredis.test;

import com.zqh.nosql.redis.springredis.LocalRedisConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;

import static org.junit.Assert.assertNotNull;

@ContextConfiguration(classes = LocalRedisConfig.class)
@RunWith(SpringJUnit4ClassRunner.class)
public class LocalRedisConfigTest {

	@Inject
	JedisConnectionFactory jedisConnectionFactory;
	
	@Inject
	StringRedisTemplate redisTemplate;
	
	@Test
	public void testJedisConnectionFactory() {
		assertNotNull(jedisConnectionFactory);
	}

	@Test
	public void testRedisTemplate() {
		assertNotNull(redisTemplate);
	}

}
