package sparkstreaming_action.userbehavior.main

import sparkstreaming_action.userbehavior.service.RedisService
import sparkstreaming_action.userbehavior.util.Conf
import sparkstreaming_action.userbehavior.util.Tool
import sparkstreaming_action.userbehavior.util.RedisUtil

/** 
 *  用户行为日志统计结果展示
 */
object StatisticResults extends App {
  
  // Redis 键（用户）模式匹配串
  val userPattern = "*" + Conf.USER_SUFFIX
  // 模糊查询匹配键集合
  val keys = RedisUtil.listKeys(userPattern).get
  // 按键查询值
  val values = RedisUtil.batchGet(keys).get
  // 合并键值对
  val kvs = keys.zip(values)
  // 遍历输出
  kvs.foreach(kvs => {
    val values = Tool.decode(kvs._2)(RedisService.doRead).get
    println("\n[" + kvs._1 + "]:\n" + values.mkString("\t"))
  })
  
}