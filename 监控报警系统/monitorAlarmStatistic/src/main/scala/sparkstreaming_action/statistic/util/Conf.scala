package sparkstreaming_action.statistic.util

/** 
 *  配置信息
 */
object Conf extends Serializable {
  
  // parameters configuration
  val nGram = 3
  val updateFreq = 300000 //(ms) 5min
  
  // spark configuration
  val master = "spark://master:7077"
  val localDir = "./tmp"
//  val checkpointDir = "./checkpoint"
  val perMaxRate = "5"  // 每个分区每秒能读取的最大记录数
  val interval = 3 // seconds 流式处理间隔
  val parallelNum = "15"
  val executorMem = "1G"
  val concurrentJobs = "5"
  val coresMax = "2"
//  val partitionNum = 2
//  val batchSize = 64 // 每批处理的数据量大小
  
  // mysql configuration
  val mysqlConfig = Map(
      "url" -> "jdbc:mysql://master:3306/spark?characterEncoding=UTF-8",
      "username" -> "hadoop",
      "password" -> "123456")
  val maxPoolSize = 5
  val minPoolSize = 2
  
  // kafka configuration
  val brokers = "master:9092,slave1:9092" // Kafka集群
  val zk = "master:2181,slave1:2181" // Zookeeper集群
  val group = "MASGroup"  // 消费者组
  val topics = "monitorAlarm" // 订阅主题集合，以","分隔
  val outTopics = "monitorAlarmCount" // 发布主题集合，以","分隔
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
  
}