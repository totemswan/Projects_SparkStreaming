package sparkstreaming_action.userbehavior.service

import scala.collection.mutable.ArrayBuffer
import java.io.DataInputStream
import java.io.DataOutputStream

import sparkstreaming_action.userbehavior.util.Conf
import sparkstreaming_action.userbehavior.util.Conf.RecordType._
import sparkstreaming_action.userbehavior.util.Tool
import sparkstreaming_action.userbehavior.util.RedisUtil

/** 
 *  Redis 状态更新服务（使用Redis保存状态）
 */
object RedisService {
  
  /** 格式化用户字段数据
   *  @param user 用户名
   *  @return 返回添加后缀的用户名
   */
  def constructKey(user: _KEY): _KEY = user + Conf.USER_SUFFIX
  
  /** 更新 Redis记录状态
   *  @param _records key/values记录
   *  @return Unit
   */
  def updateStatus(_records: Seq[_RECORD]): Unit = {
    if (_records != null && _records.length > 0) {
      // 更新开始时间
      val start: Long = System.currentTimeMillis()
      // 格式化记录中的用户数据
      val records: Seq[_RECORD] = _records.map({
        case (user, newVals) => (constructKey(user), newVals)
      })
      
      // 从记录中投影出用户列表
      val users: Seq[_KEY] = records.map(_._1)
      // 从redis获取用户列表（键）对应的所有原始数据
      val option = RedisUtil.batchGet(users)
      if (option.isEmpty)
        throw new Exception("[updateStatus] rawValsArr get error.")
      val rawValsArr: Array[Array[_VALUE_ENCODE]] = option.get
      
      /** 
       *  合并新旧记录并更新
       */
      // 定义记录编码后格式
      val encodeRecords = new ArrayBuffer[_RECORD_ENCODE]()
      /** 记录-原始数据 一一合并 相当于join操作
       *    效果是：一个user 对应一条新状态和一条原状态
       *    合并后格式：((user, Array(item, time), 未解码的Byte数据))
       *    
       *    注：首次合并时，rawValsArr 为 Array(null) 数组（Redis中无记录）
       *        即：每个user 对应一个 null 
       *        格式：((user,Array(item, time)), null)
       */
      val recordsAndRawValsArr = records.zip(rawValsArr)
      // 遍历记录，更新状态
      for (((user, newVals), rawVals) <- recordsAndRawValsArr) {
        encodeRecords += updateStatusAndEncode(user, rawVals, newVals)
      }
      
      // 将更新后状态写回redis保存
      RedisUtil.batchSet(encodeRecords)
      
//      if (math.random < 0.1)
        println(s"[updateStatus] deal with ${records.length} "
          + s"records in ${System.currentTimeMillis()- start} ms.")
          
    } else {
      println("[updateStatus] _records is null!")
    }
  }
  
  /** 
   *  更新 Redis状态并编码
   *  @param key Redis键
   *  @param rawVals 原始值（Redis读出的值，rawVals!=null 只可以为null数组（首次更新时））
   *  @param newVals 新值（Kafka读入的值）
   *  @return _RECORD_ENCODE 返回编码后的更新状态
   */
  def updateStatusAndEncode(key: _KEY, 
                            rawVals: Array[_VALUE_ENCODE],
                            newVals: ArrayBuffer[_VALUE]): _RECORD_ENCODE = {
    if (newVals == null)
      throw new Exception("[updateStatusAndEncode] newVals is null.")
    
    // 对原始数据值解码
    var rawValsDecoded: Array[_VALUE] = null
    if (rawVals != null && !rawVals.isEmpty) { // 解码数据不能为null 也不能为空
      // 解码：自定义解码方式 doRead
      val decodeOp = Tool.decode(rawVals)(doRead)
      if (decodeOp.isEmpty)
        throw new Exception("[updateStatusAndEncode] rawVals decode error.")
      // 获取解码值
      rawValsDecoded = decodeOp.get
      // 控制台打印 Redis键值对信息
      if (math.random < 0.1) {
        println(s"query $key and get ${rawValsDecoded.mkString("\t")}")
      }
    }
    
    // 用新值更新原始值（原始值可以为null）
    val updatedVals: Array[_VALUE] = updateValues(rawValsDecoded, newVals)
    // 待编码数据updatedVals不能为null 但可以为空
    if (updatedVals == null)
      throw new Exception("[updateStatusAndEncode] updatedVals get error.")
    
    // 对更新后的 Redis值 updatedVals 进行编码  
    // 编码：自定义编码方式 doWrite
    val encodeOp = Tool.encode(updatedVals)(doWrite)
    if (encodeOp.isEmpty)
      throw new Exception("[updateStatusAndEncode] updatedVals encode error.")
    // 获取编码值
    val updatedValsEncoded = encodeOp.get
    
    // 返回编码后的记录更新状态 (key, values)
    (key.getBytes, updatedValsEncoded)
  }
  
  /** 
   *  更新 Redis值，只保留滑动窗口内的有效数据记录
   *  @param rawVals 原始值（Redis读出的值）
   *  @param newVals 新值（Kafka读入的值）
   *  @return 更新后的 Vals （不会为null，最多为空）
   */
  def updateValues(rawVals: Array[_VALUE], 
                   newVals: ArrayBuffer[_VALUE]): Array[_VALUE] = {
    if (newVals == null)
      throw new Exception("[updateValues] newVals is null.")
    
    /** 合并、排序（二次升序排序）
     *    (item, time) 结构的排序，先排item，后排time
     */
    var unionVals: Array[_VALUE] = null
    if (rawVals == null)
      unionVals = newVals.toArray.sorted
    else 
      unionVals = (newVals ++ rawVals).toArray.sorted
      
    /** 
     *  倒序遍历，先处理最新访问数据（即时间戳最大的记录）
     *  并且只保留滑动窗口内的有效记录
     */
    val updatedVals = ArrayBuffer[_VALUE]()
    var preItem = -1L // 前一个项目
    var cnt = 0 // 记录每个项目的访问次数（上限为MAX_CNT）
    var i = unionVals.length - 1
    while (i >= 0) {
      val (item, timestamp) = unionVals(i)
      // 验证访问时间是否过期：过滤掉滑动窗口以外的记录
      if (!Tool.isInvalidate(timestamp, Conf.windowSize)) {
        /** 对相同项目的访问次数计数
         *    限制同一项目只保存前 MAX_CNT 条访问记录
         */
        if (item == preItem) {
          cnt += 1
        } else {
          preItem = item
          cnt = 1
        }
        if (cnt <= Conf.MAX_CNT) {
          updatedVals += ((item, timestamp))
        }
      }
      i -= 1
    }
    // 数组反转，升序
    updatedVals.reverse.toArray
  }
  
  /**
   *  编码Tool.encode()的自定义编码方式
   *  @param dos 数据输出流
   *  @param any 任意类型数据对象
   *  @return Unit
   */
  def doWrite(dos: DataOutputStream, any: Any): Unit = {
    if (any.isInstanceOf[Array[_VALUE]]) {
      val values = any.asInstanceOf[Array[_VALUE]]
      // 字节数组两部分组成：tuples长度 和 tuples字节数组
      /** 输出数组长度
       *    注：如果 values.length == 0 也会写入 4 字节的 0 数据
       */
      dos.writeInt(values.length)
      // 遍历输出元组数组
      for (i <- 0 until values.length) {
        dos.writeLong(values(i)._1) // item
        dos.writeLong(values(i)._2) // time
      }
    }
  }
  
  /**
   *  解码Tool.decode()的自定义解码方式
   *  @param dis 数据输入流
   *  @return Array[_VALUE] 二元组数组
   */
  def doRead(dis: DataInputStream): Array[_VALUE] = {
    // 字节数组两部分组成：tuples长度 和 tuples字节数组
    /** 读出数组长度
     *    注：允许读出长度为 0
     */
    val len = dis.readInt()
    // 读出数据，构建二元组数组
    val values = new ArrayBuffer[_VALUE]()
    for (i <- 0 until len) {
      val item = dis.readLong().asInstanceOf[_ITEM]
      val time = dis.readLong().asInstanceOf[_TIME]
      values += ((item, time))
    }
    values.toArray
  }
}