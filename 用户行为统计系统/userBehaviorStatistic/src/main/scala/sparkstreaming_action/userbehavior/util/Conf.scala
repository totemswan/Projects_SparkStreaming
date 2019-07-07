package sparkstreaming_action.userbehavior.util

import scala.collection.mutable.ArrayBuffer

/**
 * configuration
 */
object Conf {
  
  // 定义 记录的数据字段类型
  object RecordType {
    // 业务字段类型
    type _USER = String // 用户
    type _ITEM = Long // 项目
    type _TIME = Long // 时间戳
    
    // Redis 字段类型
    type _KEY = _USER // Redis 键类型
    type _KEY_ENCODE = Byte // Redis 键的存储类型（编码为字节数据）
    
    type _VALUE = (_ITEM, _TIME) // Redis 值类型
    type _VALUE_ENCODE = Byte // Redis 值的存储类型（编码为字节数据）
    
    type _RECORD = (_KEY, ArrayBuffer[_VALUE]) // 记录
    type _RECORD_ENCODE = (Array[_KEY_ENCODE], Array[_VALUE_ENCODE]) // 记录的存储类型（编码为字节数组）
  }
  
  // spark configuration
  val master = "spark://master:7077"
  val localDir = "./tmp"
  val checkpointDir = "./checkpoint"
  val perMaxRate = "5"  // 每个分区每秒能读取的最大记录数
  val interval = 3 // seconds 流式处理间隔
  val parallelNum = "15"
  val executorMem = "1G"
  val concurrentJobs = "5"
  val coresMax = "2"
  val partitionNum = 2
  val batchSize = 64 // 每批处理的数据量大小
  
  // streaming window configuration
  val MAX_CNT = 25 // 项目访问数上限
  val EXPIRE_DURATION = 60 * 60 * 24 * 3 // 到期持续时间（有效时间），单位: s （此处为3天）
  val windowSize = 72 * 3600 // 滑动窗口大小（有效时间），单位: s （此处为3天）
  
  // kafka configuration
  val brokers = "master:9092,slave1:9092" // Kafka集群
  val zk = "master:2181,slave1:2181" // Zookeeper集群
  val group = "userBehaviorGroup"  // 消费者组
  val topics = "userBehavior" // 主题集合，以","分隔
  /**
   * auto.offset.reset 的值（取值）
   *   kafka-0.10.1.X版本之前: smallest, largest 
   *   		(offest保存在zk中)
   *   kafka-0.10.1.X版本之后: earliest, latest, none 
   *   		(offest保存在kafka的一个特殊的topic名为:__consumer_offsets里面)
   *   
   *   参考博文：https://www.cnblogs.com/peterkang202/p/10443991.html
   *     earliest： 当各分区下有已提交的offset时，从提交的offset开始消费；
   *     	    （区别点）无提交的offset时，从头开始消费
	 *	   latest： 当各分区下有已提交的offset时，从提交的offset开始消费；
	 * 				（区别点）无提交的offset时，消费新产生的该分区下的数据
	 *     none： topic各分区都存在已提交的offset时，从offset后开始消费；
	 *     		（区别点）只要有一个分区不存在已提交的offset，则抛出异常
   */
  val offsetReset = "latest"
  
  // kafka log record: String(time\tuser\titem)
  val SEPERATOR = "\t"
  // kafka log record: Array(time,user,item)
  val INDEX_LOG_TIME = 0
  val INDEX_LOG_USER = 1
  val INDEX_LOG_ITEM = 2
  // redis status record: Tuple2(item, time)
  val INDEX_STATUS_ITEM = 1
  val INDEX_STATUS_TIME = 2
  // redis status user suffix
  val USER_SUFFIX = "_freq"
  
  // redis configuration
  val redisIp = "192.168.190.200" // Redis 服务位于 master 节点
  val redisPort = 6379
  val passwd = "123456"
  
}