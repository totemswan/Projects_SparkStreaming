package sparkstreaming_action.wordfreq.main

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable.HashSet
import kafka.serializer.StringDecoder

import sparkstreaming_action.wordfreq.dao.KafkaManager
import sparkstreaming_action.wordfreq.util.BroadcastWrapper
import sparkstreaming_action.wordfreq.util.Conf
import sparkstreaming_action.wordfreq.service.MysqlService
import sparkstreaming_action.wordfreq.service.SegmentService

/** 
 *  消费主程序
 */
object ConsumerMain extends Serializable {
  
  @transient lazy val log = LogManager.getLogger(this.getClass)
  
  def functionToCreateContext(): StreamingContext = {
    // 设置Spark配置
    val sparkConf = new SparkConf()
      .setAppName("WordFreqConsumer")
      .setMaster(Conf.master)
      .set("spark.default.parallelism", Conf.parallelNum)
      .set("spark.streaming.concurrentJobs", Conf.concurrentJobs)
      .set("spark.executor.memory", Conf.executorMem)
      .set("spark.cores.max", Conf.coresMax)
      .set("spark.local.dir", Conf.localDir)
      .set("spark.streaming.kafka.maxRatePerPartition", Conf.perMaxRate)
    // 创建流式上下文
    val ssc = new StreamingContext(sparkConf, Seconds(Conf.interval))
//    ssc.checkpoint(Conf.localDir)
    
    // 获取Kafka主题topics集合
    val topics = Conf.topics.split(",").toSet
    // 获取Kafka配置
    val kafkaParams = Map[String, String](
        "metadata.broker.list" -> Conf.brokers,
        "auto.offset.reset" -> Conf.offsetReset,
        "group.id" -> Conf.group)
    // 创建Kafka数据管理层
    val km = new KafkaManager(kafkaParams)
    // 创建Kafka直接读取数据流：键值对格式 (元数据，消息)
    val kafkaDirectStream = km.createDirectStream[String, String,
        StringDecoder, StringDecoder](ssc, kafkaParams, topics)
        
    log.warn(s"Initial Done***>>>topic:${Conf.topics}\t"
        + s"group:${Conf.group}\tlocalDir:${Conf.localDir}\t"
        + s"brokers:${Conf.brokers}")
    // 缓存数据流
    kafkaDirectStream.cache()    
    
    /** 加载词频统计词库
     *  构建广播变量数据类型：
     *  (时间戳，词库) -- 时间戳用于定时更新时的时间计算
     *  (timestamp: Long, words: HashSet[String])
     */
    val words = BroadcastWrapper[(Long, HashSet[String])](ssc, 
        (System.currentTimeMillis(), MysqlService.getUserWords()))
    
    /** 对kafka每条消息进行分词操作
     *  注：直接 kafkaDirectStream.flatMap 也能实现相同功能
     *      不过定期更新词库的操作就需要在各个partition中（Worker）执行，
     *      无法在Driver中执行，并且时间判断的频次也会增高
     */
    val segmentedStream: DStream[(String, Int)] = kafkaDirectStream
      .map(record => {
        println("data: " + record)
        record._2
      })
      .repartition(10)
      .transform((rdd: RDD[String]) => { // 通过 transform 操作RDD
        // Driver中执行
        // 定期更新词库，更新完成前阻塞使用广播变量的进程
        if (System.currentTimeMillis() - words.value._1 > Conf.updateFreq) {
          words.update((System.currentTimeMillis(), MysqlService.getUserWords()), true)
          log.warn("[BroadcastWrapper] words updated")
        }
        /** 对记录分词，并按词库统计
         * RDD.flatMap[U](f: T => TraversableOnce[U]): RDD[U]
         * 此处：T 为 String  
         * 			 U 为 (String, Int)  
         * 			 TraversableOnce 子类为 Map
         */
        rdd.flatMap((record: String) => SegmentService.mapSegment(record, words.value._2))
      })
    
    // 按键（词）聚合，统计每个词的个数
    val countedWordStream = segmentedStream.reduceByKey(_ + _)
    
    // 将统计结果输出至MySQL数据库
    countedWordStream.foreachRDD(MysqlService.save(_))
    
    // 消费完一批消息（即：成功写入Mysql后），更新ZK中的 offsets
    kafkaDirectStream.foreachRDD((rdd: RDD[(String, String)]) => {
      if (!rdd.isEmpty()) {
        km.updateZKOffsets(rdd)
      }
    })
    
    ssc
  }
  
  /**
   *  程序入口
   */
  def main(args: Array[String]) {
    val ssc = functionToCreateContext()
    ssc.start()
    ssc.awaitTermination()
  }
}