package sparkstreaming_action.statistic.dao

import java.util.concurrent.Future
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord, RecordMetadata }
import java.util.Properties

class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
  
  // 避免运行时产生NotSerializableException异常
  lazy val producer = createProducer()
  
  def send(topic: String, key: K, value: V): Future[RecordMetadata] = {
    // 写入Kafka
    producer.send(new ProducerRecord[K, V](topic, key, value))
  }
  
  def send(topic: String, value: V): Future[RecordMetadata] = {
    // 写入Kafka
    producer.send(new ProducerRecord[K, V](topic, value))
  }
}

object KafkaSink {
  // 导入 scala java 自动类型互转类
  import scala.collection.JavaConversions._
  
  // 此处Map 为 scala.collection.immutable.Map
  def apply[K, V](config: Map[String, String]): KafkaSink[K, V] = {
    val createProducerFunc = () => {
      // 新建KafkaProducer
      // scala.collection.Map => java.util.Map
      val producer = new KafkaProducer[K, V](config) //需要java.util.Map
      // 虚拟机JVM退出时执行函数
      sys.addShutdownHook({
        // 确保在Executor的JVM关闭前，KafkaProducer将缓存中的所有信息写入Kafka
        // close()会被阻塞直到之前所有发送的请求完成
        producer.close()
      })
      producer
    }
    new KafkaSink[K, V](createProducerFunc)
  }
  
  // 隐式转换 java.util.Properties => scala.collection.mutable.Map[String, String]
  // 再通过 Map.toMap => scala.collection.immutable.Map
  def apply[K, V](config: Properties): KafkaSink[K, V] = apply(config.toMap)
}
