package sparkstreaming_action.crawler.main

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import kafka.utils.Logging
import java.util.Properties
import java.util.Collections
import java.util.concurrent.Executors
import java.time.Duration
import java.util.Set

/** 测试生产数据的消费者
 *  
 * @param brokers 引导服务器列表
 * @param groupId 消费者组
 * @param topic 主题
 */
class ConsumerTest(val brokers: String,
                   val groupId: String,
                   val topic: String) extends Logging{
  // 消费者配置
  val props = createConsumerConfig(brokers, groupId)
  //构建Kafka消费者
  val consumer = new KafkaConsumer[String, String](props)
  
  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props
  }
  
  def run() = {
    // 消费者订阅主题（构建一个不可变的可序列化列表）
    consumer.subscribe(Collections.singletonList(this.topic))
    //创建单线程执行器
    //一个消费者一个线程，多个消费者需要开启多个线程
    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        //轮询：不断拉取指定topic的数据，并打印输出查看
        while (true) {
          //如果有数据立刻返回；如果没有数据，阻塞等待timeout时间(s)
          val records = consumer.poll(Duration.ofSeconds(1000)).iterator()
          while (records.hasNext()) {
            val record = records.next()
            // 打印记录
            println("Received message: (" + record.key() + "," + record.value() + ")" 
                + " at offset " + record.offset())
          }
        }
      }
    })
  }
}

object Consumer extends App {
  val example = new ConsumerTest(args(0), args(1), args(2))
  example.run()
}