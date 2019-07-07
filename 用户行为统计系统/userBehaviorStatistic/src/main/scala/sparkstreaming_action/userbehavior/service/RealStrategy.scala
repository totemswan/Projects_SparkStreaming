package sparkstreaming_action.userbehavior.service

import scala.collection.mutable.ArrayBuffer
import sparkstreaming_action.userbehavior.util.Tool

trait RealStrategy extends Serializable {
  
  // 时间有效范围
  val TRIM_DURATION = 3600 * 72 // 单位：s （此处为72小时）
  
  /**
   *  获取关键字字段下标数组
   *  @return Array[Int]
   */
  def getKeyFields: Array[Int]
  
  /** 
   *  更新状态（时间戳数组）
   *    合并新旧状态的时间戳
   *  @param logs: Seq[Array[String]] 日志数据，每条日志结构(timestamp, user, item)
   *  @param previous: ArrayBuffer[Long] 原有时间戳数组
   *  @return ArrayBuffer[Long] 更新后的时间戳数组
   */
  def updateStatus(logs: Seq[Array[String]], previous: ArrayBuffer[Long]): ArrayBuffer[Long]
  
  /**
   *  去除时间戳数组中已经过期的时间戳
   *  @param timestamps: ArrayBuffer[Long] 时间戳数组
   *  @return ArrayBuffer[Long] trim后的时间戳数组
   */
  def trim(timestamps: ArrayBuffer[Long]): ArrayBuffer[Long] = trimHelper(timestamps, TRIM_DURATION)
  
  /**
   *  去除时间戳数组中已经过期的时间戳
   *  @param timestamps: ArrayBuffer[Long] 时间戳数组（默认升序），单位：s
   *  @param duration: Long 持续时间（有效时间范围），单位：s
   *  @return ArrayBuffer[Long] 返回有效的时间戳数组
   */
  def trimHelper(timestamps: ArrayBuffer[Long],
                 duration: Long): ArrayBuffer[Long] = {
    // 遍历时间戳数组（默认升序），找出过期时间戳与有效时间戳的分界点（数组索引位置）
    var i = 0
    while (i < timestamps.length && Tool.isInvalidate(timestamps(i), duration))
      i += 1
    // 从最后一个过期时间戳位置处开始，向后截取出有效的时间戳 
    timestamps.slice(i, timestamps.length)
  }
  
}