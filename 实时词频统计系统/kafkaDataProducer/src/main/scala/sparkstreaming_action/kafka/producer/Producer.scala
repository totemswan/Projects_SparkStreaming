package sparkstreaming_action.kafka.producer

import scala.util.Random
import scala.io.Source
import java.util.Properties
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.ProducerConfig


// 用于生成模拟数据的生产者
object Producer extends App{
  
  // 从命令行接收参数
  val eventsNum = args(0).toInt  // 评论事件数目
  val topic = args(1)   // 主题
  val brokers = args(2)  // 引导服务器列表
  
  // 添加配置项
  val props = new Properties()
  props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "kafkaDataProducer")
  /** 注：不要用 classOf[StringSerializer].toString()，要用 .getName()
   *  toString() 输出：class org.apache.kafka.common.serialization.StringSerializer（多了开头class）
   *  getName() 输出：org.apache.kafka.common.serialization.StringSerializer
   */
  props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  
  // 构建Kafka生产者
  val producer = new KafkaProducer[String, String](props)
  // 开始生产时间
  val startTime = System.currentTimeMillis()
  val rnd = new Random()
  
  // 读取汉字字典
  val source = Source.fromFile("./hanzi.txt")
  val lines = try source.mkString finally source.close()
  for (nEvents <- Range(0, eventsNum)) {
    // 生成模拟评论数据 (user, comment)
    val sb = new StringBuilder()
    // 随机从字典中抽取200个以内汉字拼在一起
    for (index <- Range(0, rnd.nextInt(200))) {
      sb += lines.charAt(rnd.nextInt(lines.length()))
    }
    // 构建用户（100个以内）
    val userName = "user_" + rnd.nextInt(100)
    // 构建生产者记录
    val data = new ProducerRecord[String, String](topic, userName, sb.toString())
    //异步向Kafka发送记录
    producer.send(data, new Callback() {
        //实现发送完成后的回调方法
        override def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
          if(e != null) {
            e.printStackTrace();
          } else {
            println("The offset of the record we just sent is: " + metadata.offset());
          }
        }
    });
  }
  
  // 计算每条记录的平均发送时间
  println("sent per second: " + (eventsNum * 1000 / (System.currentTimeMillis() - startTime)))
  producer.close()
}