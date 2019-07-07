package sparkstreaming_action.userbehavior.main

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.collection.mutable.ArrayBuffer

import sparkstreaming_action.userbehavior.util.Conf
import sparkstreaming_action.userbehavior.service.RedisService
import org.apache.log4j.LogManager

/**
 *  程序入口
 */
object RealFeatureStatistic {
  def main(args: Array[String]): Unit = {
    val realFeature = new RealFeatureStat
    realFeature.train
  }
}

class RealFeatureStat extends Serializable {
  
  def constructKVDStream(ssc: StreamingContext): DStream[(String, Array[String])] = {
    // kafka 参数配置
    val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> Conf.brokers,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> Conf.group,
        "auto.offset.reset" -> Conf.offsetReset,
        "enable.auto.commit" -> (false: java.lang.Boolean))
    
    // 创建 kafka 直接读取数据流
    val directStream: InputDStream[ConsumerRecord[String, String]] 
      = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](List(Conf.topics), kafkaParams))
    
    /**
     *  从数据流 (user, "timestamp\tuser\titem")
     *  映射为(user, Array(timestamp, user, item)) 数据流
     */
    val KVDStream: DStream[(String, Array[String])] = directStream.map(record => {
      // 将 ConsumerRecord[String, String] 的value值 分割 为数组
      val arr = record.value().split(Conf.SEPERATOR)
      (record.key(), arr)
    })
    
    KVDStream
  }
  
  def createContext: StreamingContext = {
    // 创建 Spark 上下文
    val sc = SparkSession
      .builder()
      .master(Conf.master)
      .appName("realStatistic")
      .getOrCreate()
      .sparkContext
      
    // 创建流式上下文
    val ssc = new StreamingContext(sc, Seconds(Conf.interval))
    /** 设置检查点数据保存路径
     *    实现对kafka偏移量offsets的自动保存和断点恢复
     *    注：目录不存在时会自动创建
     */
    ssc.checkpoint(Conf.checkpointDir)
    
    // 读取kafka数据流
    val view: DStream[(String, Array[String])] = constructKVDStream(ssc)
    // 按键（用户名）分组
    val kSeq: DStream[(String, Iterable[Array[String]])] = view.groupByKey()
    // 缓存
    kSeq.cache()

    kSeq.foreachRDD((rdd: RDD[(String, Iterable[Array[String]])]) => {
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(partition => {
          // 每个分区按批执行（没批更新数据条数不超过batchSize）
          while (partition.hasNext) {
            val recordsBuf = new ArrayBuffer[(String, ArrayBuffer[(Long, Long)])]
            while (partition.hasNext && recordsBuf.size < Conf.batchSize) {
              // 取单条 用户操作记录records: Iterable[Array(time,user,item)]
              val (key, values) = partition.next()
              /** 分离出该用户的所有操作记录(item, time)
               *    利用 scala.collection.breakOut
               *    从 Iterable[(Long, Long)] 转为 ArrayBuffer[(Long, Long)]
               */
              val newVals: ArrayBuffer[(Long, Long)] = values.map(value => {
                // 用户访问的项目
                val item = value(Conf.INDEX_LOG_ITEM).toLong
                // 用户访问项目的时间
                val time = value(Conf.INDEX_LOG_TIME).toLong
                // 返回
                (item, time)
              })(scala.collection.breakOut)
              recordsBuf += ((key, newVals))
            }
            try {
              // 更新用户行为状态
              RedisService.updateStatus(recordsBuf)
            } catch {
              case e: Exception => 
                println("[RedisService_updateStatusError]", e)
            }
          }
          
        })
      }
    })
    
    ssc
  }
  
  def train: Unit = {
    /** 从检查点数据中重建StreamingContext或者新建一个
     *    如果checkpointDir存在，则从检查点数据重建流式上下文
     *    若果不存在，则调用函数 createContext 创建
     */
    val ssc = StreamingContext.getOrCreate(Conf.checkpointDir, createContext _)
    ssc.start()
    ssc.awaitTermination()
  }
}