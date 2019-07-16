package sparkstreaming_action.statistic.main

import java.util.Properties
import scala.collection.mutable.Map

import org.apache.log4j.LogManager

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.consumer.ConsumerRecord

import sparkstreaming_action.statistic.dao.KafkaSink
import sparkstreaming_action.statistic.entity.MonitorGame
import sparkstreaming_action.statistic.service.MysqlService
import sparkstreaming_action.statistic.service.SegmentService
import sparkstreaming_action.statistic.util.BroadcastWrapper
import sparkstreaming_action.statistic.util.Conf

object MonitorAlarmStatistic {
  
  @transient lazy val log = LogManager.getLogger(this.getClass)
  
  def functionToCreateContext(): StreamingContext = {
    // 设置Spark配置
    val sparkConf = new SparkConf()
      .setAppName("MonitorAlarmStatistic")
      .setMaster(Conf.master)
      .set("spark.default.parallelism", Conf.parallelNum)
      .set("spark.streaming.concurrentJobs", Conf.concurrentJobs)
      .set("spark.executor.memory", Conf.executorMem)
      .set("spark.cores.max", Conf.coresMax)
      .set("spark.local.dir", Conf.localDir)
      .set("spark.streaming.kafka.maxRatePerPartition", Conf.perMaxRate)
    // 创建流式上下文
    val ssc = new StreamingContext(sparkConf, Seconds(Conf.interval))
//    ssc.checkpoint(Conf.checkpointDir)
    
    // kafka 参数配置
    val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> Conf.brokers,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> Conf.group,
        "auto.offset.reset" -> Conf.offsetReset,
        "enable.auto.commit" -> (false: java.lang.Boolean))
    /** 创建 kafka 直接读取数据流
     *    1) ConsumerStrategies 消费策略有三大类：
     *    	1. Subscribe 订阅主题，动态分配分区
     *      2. SubscribePattern 订阅模式匹配主题，动态分配分区
     *      3. Assign 固定分配主题分区
     *      注：Driver和Executor使用相同的 kafkaParams
     *      注：无offsets指定时，使用 committed offset（如果能获取到） 或者 auto.offset.reset
     *    2) LocationStrategies 本地（Executor）调度消费者策略有三大类：
     *      1. PreferBrokers 只有Executors与Kafka brokers位于相同节点时使用
     *      2. PreferConsistent （大多数情况下使用）会一直在所有Executors间分发分区
     *      3. PreferFixed 除非负载不均衡时，指定某主机使用固定分区；如果未指定分区，将转为 PreferConsistent策略
     *      注. 同一Executor使用缓存 consumers ，而不是在各个分区中重新创建，以提升性能
     */
    val kafkaDirectStream: InputDStream[ConsumerRecord[String, String]] 
      = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](List(Conf.topics), kafkaParams))
        
    log.warn(s"Initial Done***>>>topic:${Conf.topics}\t"
        + s"group:${Conf.group}\tlocalDir:${Conf.localDir}\t"
        + s"brokers:${Conf.brokers}")
//    // 缓存数据流
//    kafkaDirectStream.cache()    
    
    /** 广播监控游戏库
     *    广播变量数据类型：
     *    (时间戳，监控游戏库) -- 时间戳用于定时更新时的时间计算
     *    (timestamp: Long, games: Map[Int, MonitorGame])
     */
    val monitorGames = BroadcastWrapper[(Long, Map[Int, MonitorGame])](ssc, 
        (System.currentTimeMillis(), MysqlService.getMonitorGames))
    
    /** 广播KafkaSink
     *    用于每个计算节点向Kafka发布结果数据
     *    这样设计的解释：
     *      1. 如果不这样做，那么每个分区记录在向kafka发布消息时，都得重新创建生产者
     *         以至于增加重复创建的开销；
     *      2. 广播生产者至每个计算结点缓存，能使各个分区使用同一个生产者对象，避免了重复创建的开销。
     *      3. ** 同样的设计也出现在 createDirectStream() 的消费者策略中。
     */
    val kafkaProducer = BroadcastWrapper[KafkaSink[String, String]](ssc, 
        KafkaSink[String, String]({
          val kafkaProducerConfig = {
            val p = new Properties()
            p.setProperty("bootstrap.servers", Conf.brokers)
            p.setProperty("key.serializer", classOf[StringSerializer].getName)
            p.setProperty("value.serializer", classOf[StringSerializer].getName)
            p
          }
          kafkaProducerConfig
        }))
        
    /** 对kafka每条消息进行分词操作
     *    返回格式：(gameId, 拆分的评论词) (Int, String)
     *    ** 考虑是否在此处将消息记录先过滤一下，去除可能存在的HTML标签
     */
    val segmentedStream: DStream[(Int, String)] = kafkaDirectStream
      .map(_.value())
      .transform((rdd: RDD[String]) => { // 通过 transform 操作RDD
        // Driver中执行
        // 定期更新监控游戏库，更新完成前阻塞使用广播变量的进程
        if (System.currentTimeMillis() - monitorGames.value._1 > Conf.updateFreq) {
          monitorGames.update((System.currentTimeMillis(), MysqlService.getMonitorGames), true)
          log.warn("[BroadcastWrapper] monitorGames updated")
        }
        rdd.map((reviewJson: String) => {
          // 对需要监控的游戏评论进行分词
          val option: Option[(Int, String)] = SegmentService.mapSegment(reviewJson, monitorGames.value._2)
          // 分词结果可能会存在 null(None) 数据 
          if (option.isEmpty) 
            null
          else 
            option.get
        })
      })
    
    // 输出到 Kafka
    segmentedStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreach(record => {
          // 分词结果不为null时才进行处理
          if (record != null) {
            // 元数据（键）gameId, 消息（值）jsonRecord
            kafkaProducer.value.send(Conf.outTopics, record._1.toString(), record._2)
            log.warn(s"[KafkaOutput] output to ${Conf.outTopics}\tgameId: ${record._1}")
          }
        })
      }
    })
    
    ssc
  }
  
  /**
   *  程序入口
   */
  def main(args: Array[String]) {
    // 因为有广播变量无法使用Checkpoint
    val ssc = functionToCreateContext()
    ssc.start()
    ssc.awaitTermination()
  }
}