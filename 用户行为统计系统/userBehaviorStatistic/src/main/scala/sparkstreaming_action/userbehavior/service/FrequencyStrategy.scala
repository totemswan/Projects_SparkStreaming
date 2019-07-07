package sparkstreaming_action.userbehavior.service

import sparkstreaming_action.userbehavior.util.Conf
import scala.collection.mutable.ArrayBuffer

/**
 * 利用mapWithState时需要依赖的State设计
 * （此项目中没有使用，而是用redis存储设计）
 */
class FrequencyStrategy extends RealStrategy {
  
  // 状态数组的时间戳数量上限
  val MAX = 25
  
  /**
   *  获取关键字字段下标数组
   *  @return Array[Int]
   */
  override def getKeyFields: Array[Int] = Array(Conf.INDEX_LOG_USER, Conf.INDEX_LOG_ITEM)
  
  /** 
   *  更新状态（时间戳数组）
   *    合并新旧状态的时间戳
   *  @param logs: Seq[Array[String]] 日志数据，每条日志结构(timestamp, user, item)
   *  @param previous: ArrayBuffer[Long] 原有时间戳数组
   *  @return ArrayBuffer[Long] 更新后的状态（时间戳数组）
   */
  override def updateStatus(logs: Seq[Array[String]],
                      previous: ArrayBuffer[Long]): ArrayBuffer[Long] = {
    /** ArrayBuffer解读：
     *    使用ArrayBuffer 可以无限制（相对Array而言，有Int.MAX上限）尾加操作
     *    应用于：大数据量的尾加操作
     *    原理：数组复制
     *    初始大小：16
     *    扩展方式：翻倍
     */
    // 从日志的三维结构中分解出时间戳维度
    var logsTime: Seq[Long] = logs.map(_(Conf.INDEX_LOG_TIME).toLong)
    // 日志条数超过 MAX 的指定数量，则输出提示
    if (logsTime.length > MAX) {
      println("exceed max length:\n" + logs.map(_.mkString("\t")).mkString("\n"))
    }
    /** 截取最后MAX条日志
     *    如果长度 <= MAX，则取所有记录（slice()的起始from可以为负值，相当于从0开始）
     *    如果长度 > MAX，则取最后MAX条记录
     */
    logsTime = logsTime.slice(logsTime.length - MAX, logsTime.length)
    
    // 副本：原先的时间戳数组
    val x = previous
    // 旧状态：去除已过时的时间戳
    var status = trim(x)
    
    // 这个概率非常小
    if (math.random < 0.00005) {
      println(s"[processed]: qq:${logs(0)(Conf.INDEX_LOG_ITEM)}," 
        + s" order:${logs(0)(Conf.INDEX_LOG_USER)}${logsTime(0)}"
        + s" at ${System.currentTimeMillis() / 1000},"
        + s" x=${status.mkString(",")}")
    }
    
    /** 合并新旧状态的时间戳
     *    新旧状态的时间戳合并前，需保证合并后的最大数量不超过MAX
     *    如果超过了，去除旧状态中的超过数量个时间戳
     */
    val shouldRemove = logsTime.length + status.length - MAX
    if (shouldRemove > 0) {
      for (i <- 0 until shouldRemove if status.length > 0)
        status.remove(0)
    }
    // 合并
    status ++= logsTime
    
    // 返回合并后的状态
    status
  }
  
  /**
   *  删除数组中比指定元素小的前几个元素
   *  @param arr: ArrayBuffer[Long] 数组（默认升序）
   *  @param elem: Long 指定元素
   *  @return ArrayBuffer[Long] 删除后的数组
   */
  def remove(arr: ArrayBuffer[Long], elem: Long): ArrayBuffer[Long] = {
    while (arr.length > 0 && arr(0) <= elem)
      arr.remove(0)
    arr
  }
  
}