package sparkstreaming_action.userbehavior.dao

import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.LogManager

import sparkstreaming_action.userbehavior.util.Conf
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.exceptions.JedisException

/**
 *  *Redis 客户端编程 主页：https://redis.io/clients
 *  以下是相关推荐：
 *  （推荐）jedis 源码及教程：https://github.com/xetorthio/jedis/wiki
 *          jedis 连接池：https://github.com/xetorthio/jedis/wiki/Getting-started (using Jedis in a multithreaded environment)
 *          oschina jedis 2.1.0 API 使用手册：http://tool.oschina.net/uploads/apidocs/redis/clients/jedis/Jedis.html        
 *  （推荐）scala-redis 源码：https://github.com/debasishg/scala-redis
 *  spark-redis 源码：https://github.com/RedisLabs/spark-redis/blob/master/doc/getting-started.md
 *  sedis 源码：https://github.com/pk11/sedis
 */

/** 
 *  Redis connection Pool
 */
class RedisPool extends Serializable {
  
  // Jedis 连接池：超时时间5000ms
  lazy private val pool =  new JedisPool(new JedisPoolConfig, Conf.redisIp,
                                         Conf.redisPort, 5000, Conf.passwd)
  
  // 获取连接
  def getConn: Jedis = synchronized {
    pool.getResource
  }
  
  // 释放连接
  def disConn(jedis: Jedis): Unit = {
    if (jedis != null && jedis.isConnected()) {
      jedis.close()
    }
  }
  
}

/** 
 *  Redis Manager 惰性单例
 */
object RedisManager {
  
  @volatile private var pool: RedisPool = _
  
  def getPool: RedisPool = {
    if (pool == null) {
      synchronized {
        if (pool == null) {
          pool = new RedisPool
        }
      }
    }
    pool
  }
  
}

object RedisManagerTest extends App {
  val jedis = RedisManager.getPool.getConn
  println("jedis is getted.")
  jedis.set("test_redis", "test value")
  println("jedis is set key: test_redis " + " value: " + jedis.get("test_redis"))
  RedisManager.getPool.disConn(jedis)
  println("jedis is closed.")
}