package sparkstreaming_action.wordfreq.dao

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkException
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.kafka.clients.KafkaClient
import scala.reflect.ClassTag
import kafka.serializer.Decoder
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata

/** 
 *  Kafka 数据管理层
 */
class KafkaManager(val kafkaParams: Map[String, String]) extends Serializable {
  
  @transient lazy val log = LogManager.getLogger(this.getClass)
  private val kc = new KafkaCluster(kafkaParams)
  
  /** 创建直接数据流
   *  @param ssc spark流式上下文
   *  @param kafkaParams kafka参数
   *  @param topic 主题
   *  @param K 键类型
   *  @param V 值类型
   *  @param KD 键反序列化类型
   *  @param VD 值反序列化类型
   *  @return 直接读取数据流
   */
  def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag] ( ssc: StreamingContext, kafkaParams: Map[String, String],
    topics: Set[String]): InputDStream[(K, V)] = {
    
    // 获取消费者组id
    val groupId = kafkaParams.get("group.id").get
    // 在ZK上读取offsets前，先根据实际情况更新offsets
    setOrUpdateOffsets(topics, groupId)
    //从ZK上读取offset开始消费message
    val message: InputDStream[(K, V)] = {
      // 获取主题下的分区
      val partitionsE = kc.getPartitions(topics)
      if (partitionsE.isLeft) 
        throw new SparkException("get kafka partition failed: "
            + s"${partitionsE.left.get.mkString("\n")}")
      val partitions = partitionsE.right.get 
      // 获取各分区上的消费偏移（最新的偏移，也是此次读取的开始偏移）
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      if (consumerOffsetsE.isLeft) 
        throw new SparkException("get kafka consumer offsets failed: "
            + s"${consumerOffsetsE.left.get.mkString("\n")}")
      val consumerOffsets = consumerOffsetsE.right.get
      /** 创建直接读取数据流
       *    根据起始偏移创建 fromOffsets
       *    偏移范围是 一个批batch读取的偏移数
       *    结束偏移untilOffsets 由Driver自动结算得到
       */
      KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](
          ssc, kafkaParams, consumerOffsets, 
          (mmd: MessageAndMetadata[K, V]) => (mmd.key(), mmd.message()))
    }
    message
  }
  
  /** 创建数据流前，根据实际情况更新消费offsets
   *  @param topics
   *  @param groupId
   */
  private def setOrUpdateOffsets(topics: Set[String], groupId: String): Unit = {
    // 遍历topics
    topics.foreach(topic => {
      // 是否已被消费过
      var hasConsumed = true
      /** 从kafka获取指定主题下的所有分区
       *    返回实例 Either[left, right]，通常约定：left包装错误信息，right包装处理成功信息
       *    1. 返回实例有两种：Left 和 Right ，都继承自 Either
       *    2. 如果返回 Left 实例，则通过 父类 Either.isLeft 判断是否为Left实例，显然是 true，表示执行失败；
       *    3. 如果返回 Right 实例，则通过 父类 Either.isLeft 判断是否为Left实例，显然是 false，表示执行成功；
       *    		此时可以通过 Either.right 来获取成功返回的信息。
       */
      val partitionsE: Either[KafkaCluster.Err, Set[TopicAndPartition]] 
        = kc.getPartitions(Set(topic))
      if (partitionsE.isLeft) 
        throw new SparkException("get kafka partition failed: " 
            + s"${partitionsE.left.get.mkString("\n")}")
      val partitions: Set[TopicAndPartition] = partitionsE.right.get
      
      // 从kafka获取各分区上的消费者消费到的偏移offsets
      val consumerOffsetsE: Either[KafkaCluster.Err, Map[TopicAndPartition, Long]] 
        = kc.getConsumerOffsets(groupId, partitions)
      if (consumerOffsetsE.isLeft) 
        hasConsumed = false  // 没有消费过时，便获取不到消费偏移
      log.info("consumerOffsetsE.isLeft: " + consumerOffsetsE.isLeft)
      
      // 消费过的情况
      if (hasConsumed) {
        log.warn("消费过")
        // 获取各分区起始位置偏移
        val earliestLeaderOffsetsE
          : Either[KafkaCluster.Err, Map[TopicAndPartition, KafkaCluster.LeaderOffset]]
          = kc.getEarliestLeaderOffsets(partitions)
        if (earliestLeaderOffsetsE.isLeft)
          throw new SparkException("get earliest offsets failed: "
              + s"${earliestLeaderOffsetsE.left.get.mkString("\n")}")
        val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
        val consumerOffsets = consumerOffsetsE.right.get

        /** 存放更新到起始位置的偏移offsets
         *    可能只是存在部分分区consumerOffsets过时，
         *	   所以只更新过时分区的consumerOffsets为earliestLeaderOffsets
         *    默认定义不可变集合，插入新元素KV时会创建新的Map
         */
        var offsets: Map[TopicAndPartition, Long] = Map()
        /** 遍历消费偏移offsets集合
         *    如果zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除，
         *    针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小。
         *    如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时，
         *    这是把consumerOffsets更新为earliestLeaderOffsets 
         */
        consumerOffsets.foreach({
          case (tp, n) => {
            /**  获取消费分区tp上的起始偏移earliestLeaderOffset
             *   LeaderOffset数据结构：
             *     case class LeaderOffset(host: String, port: Int, offset: Long)
             *   TopicAndPartition数据结构：
             *     case class TopicAndPartition(topic: String, partition: Int)
             */
            val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
            /** 如果消费偏移 小于 起始偏移，说明消费偏移已过时，需更新为起始偏移
             *  否则，会重复消费消息
             */
            if (n < earliestLeaderOffset) {
              log.warn("consumer group:" + groupId + ",topic:" + tp.topic
                  + ",partition:" + tp.partition + " offsets已经过时，更新为 "
                  + earliestLeaderOffset)
              // 加入需更新（已过世）的偏移offsets
              offsets += (tp -> earliestLeaderOffset)
            }
          }
        })
        log.warn("offsets: " + offsets)
        // 如果存在过时offsets，则更新到Kafka
        if (!offsets.isEmpty) {
          kc.setConsumerOffsets(groupId, offsets)
        }
      } else { // 没有消费过的情况
        log.warn("没有消费过")
        /** 根据 auto.offset.reset 属性设置 更新偏移（头或尾）到Kafka
         *  该属性指定：消费者在读取一个没有偏移量的分区或者偏移量无效的情况下
         *  （因为消费者长时间失效，包含偏移量的记录已经过时并被删除）的处理方式。
         *  默认值：largest 从最新记录开始读取
         *  另一个值：smallest 从起始位置读取分区记录
         */
        val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase())
        var leaderOffsets: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = null
        if (reset == Some("smallest")) {
          leaderOffsets = kc.getEarliestLeaderOffsets(partitions).right.get
        } else {
          leaderOffsets = kc.getLatestLeaderOffsets(partitions).right.get
        }
        // 转换成指定格式
        val offsets = leaderOffsets.map({
          case (tp, n) => (tp, n.offset)
        })
        log.warn("offsets: " + offsets)
        kc.setConsumerOffsets(groupId, offsets)
      }
    })
  }
  
  /** 消费成功后，更新ZK上的消费offsets
   *  @param rdd 
   */
  def updateZKOffsets(rdd: RDD[(String, String)]): Unit = {
    val groupId = kafkaParams.get("group.id").get
    /** 获取从Kafka DirectStream 中 RDD 的各分区偏移(offset)范围列表
     *  OffsetRange: （分区的offset 起始from--终止until）
     */
    val offsetsList: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    for (offsets <- offsetsList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      // 向kafka发送成功消费的信号，并将读到的各分区的最新偏移（末尾偏移）更新回kafka
      val o = kc.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
      if (o.isLeft) {
        log.error(s"Error updating the offset to Kafka cluster: ${o.left.get}")
      }
    }
  }
}