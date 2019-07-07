package sparkstreaming_action.userbehavior.util

import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.LogManager
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException
import sparkstreaming_action.userbehavior.dao.RedisManager

/**
 *  Redis 工具
 */
object RedisUtil {
  
  @transient lazy val log = LogManager.getLogger(this.getClass)
  
  /** 键值对 批量 写入 Redis 存储
   *  @param kvs 键值对二元组列表（键值对需转成字节数组存储）
   *  @return Unit
   */
  def batchSet(kvs: Seq[(Array[Byte], Array[Byte])]): Unit = {
    if (kvs != null) {
      // Redis 写连接
      var writeJedis: Jedis = null
      try {
        // 获取连接
        writeJedis = RedisManager.getPool.getConn
        // 遍历键值对列表
        var i = 0
        while (i < kvs.length) {
          /**
           *  每 batchSize 条插入命令，作为一个批次执行
           *  每一批次执行，通过获取一个管道实现
           */
          // 获取管道，便于一次执行大量命令
          val pipeline = writeJedis.pipelined()
          // 每批发送指定数量（batchSize）命令
          val target = i + Conf.batchSize
          while (i < target && i < kvs.length) {
            // 插入键值对命令
            pipeline.set(kvs(i)._1, kvs(i)._2)
            // 设置键超时命令（意味着此键是不稳定的，到期后键会被 Redis server 删除）
            pipeline.expire(kvs(i)._1, Conf.EXPIRE_DURATION)
            i += 1
          }
          // 读取所有指令响应同步到管道，并关闭此管道
          pipeline.sync()
        }
      } catch {
        case connEx: JedisConnectionException => 
          log.error("[batchSet_connError]", connEx)
        case ex: Exception => 
          log.error("[batchSet_Error]", ex)
      } finally {
        RedisManager.getPool.disConn(writeJedis)
      }
    }
  }
  
  /** 按键 批量 读出 Redis 存储值
   *  @param keys 键列表
   *  @return 批量返回键对应的值列表（字节数组列表，Option包装）
   */
  def batchGet(keys: Seq[String]): Option[Array[Array[Byte]]] = {
    if (keys == null) {
      None
    } else {
      // Redis 读连接
      var readJedis: Jedis = null
      // 定义返回的值列表
      val res = ArrayBuffer[Array[Byte]]()
      try {
        // 获取连接
        readJedis = RedisManager.getPool.getConn
        // 遍历键列表
        var i = 0
        while (i < keys.length) {
          // 获取键的值
          val resp = readJedis.get(keys(i).getBytes)
          // resp 可以以 null 值 加入 res 中
          res += resp
          i += 1
        }
        Option(res.toArray)
      } catch {
        case connEx: JedisConnectionException => 
          log.error("[batchGet_connError]", connEx)
          None
        case ex: Exception => 
          log.error("[batchGet_Error]", ex)
          None
      } finally {
        RedisManager.getPool.disConn(readJedis)
      }
    }
  }
  
  /** 模式匹配查询Redis 中匹配的键集合
   *  @pattern 模式匹配串
   *  @return 返回键集合（Option包装，数组形式返回）
   */
  def listKeys(pattern: String): Option[Array[String]] = {
    if (pattern == null) {
      None
    } else {
      // Redis 读连接
      var readJedis: Jedis = null
      try {
        // 获取连接
        readJedis = RedisManager.getPool.getConn
        // 获取Redis中模式匹配的keys列表
        val keySet = readJedis.keys(pattern)
        /** 将 Set 转 Array
         *    需指定数组创建大小为Set大小
         *    避免数据量大时，对Array不断扩容，影响性能
         */
        val keyArr = new Array[String](keySet.size())
        keySet.toArray(keyArr)
        
        Option(keyArr)
      } catch {
        case connEx: JedisConnectionException => 
          log.error("[listKeys_connError]", connEx)
          None
        case ex: Exception => 
          log.error("[listKeys_Error]", ex)
          None
      } finally {
        RedisManager.getPool.disConn(readJedis)
      }
    }
  }
}