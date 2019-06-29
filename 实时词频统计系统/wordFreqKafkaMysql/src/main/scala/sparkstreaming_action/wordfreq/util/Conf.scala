package sparkstreaming_action.wordfreq.util

/**
 * configuration
 */
object Conf {
  // parameters configuration
  val nGram = 3
  val updateFreq = 300000 //(ms) 5min
  
  // api configuration
  val segmentorHost = "http://master:8282" // 结巴分词服务
  
  // spark configuration
  val master = "spark://master:7077"
  val localDir = "./tmp"
  val perMaxRate = "5"  // 每个分区每秒能读取的最大记录数
  val interval = 3 // seconds 流式处理间隔
  val parallelNum = "15"
  val executorMem = "1G"
  val concurrentJobs = "5"
  val coresMax = "2"
  
  // kafka configuration
  val brokers = "master:9092,slave1:9092" // Kafka集群
  val zk = "master:2181,slave1:2181" // Zookeeper集群
  val group = "wordFreqGroup"  // 消费者组
  val topics = "test" // 主题集合，以","分隔
  /**
   * kafka-0.10.1.X版本之前: auto.offset.reset 的值为smallest,largest.(offest保存在zk中)
   * kafka-0.10.1.X版本之后: auto.offset.reset 的值更改为:earliest,latest,none (offest保存在kafka的一个特殊的topic名为:__consumer_offsets里面)
   */
  val offsetReset = "smallest"
  
  // mysql configuration
  val mysqlConfig = Map(
      "url" -> "jdbc:mysql://master:3306/spark?characterEncoding=UTF-8",
      "username" -> "hadoop",
      "password" -> "123456")
  val maxPoolSize = 5
  val minPoolSize = 2
  
}