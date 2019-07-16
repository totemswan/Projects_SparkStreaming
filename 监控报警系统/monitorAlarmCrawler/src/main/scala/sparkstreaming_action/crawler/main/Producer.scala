package sparkstreaming_action.crawler.main

import java.util.Properties
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import spray.json._
import DefaultJsonProtocol._
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata

/** 
 *  爬虫数据生产者（消息发布到Kafka）
 *  @param args(0) pageNumPerGame 每个游戏需要爬取的页数
 *  @param args(1) topic 消息发布主题
 *  @param args(2) brokers kafka引导服务列表
 */
object Producer extends App {
  // 输入参数
  val pageNumPerGame = args(0).toInt
  val topic = args(1)
  val brokers = args(2)
  
  // kafka 配置参数
  val props = new Properties()
  props.setProperty("bootstrap.servers", brokers)
  props.setProperty("client.id", "monitorAlarmCrawler")
  props.setProperty("key.serializer", classOf[StringSerializer].getName)
  props.setProperty("value.serializer", classOf[StringSerializer].getName)
  
  // 创建 Kafka 生产者
  val producer = new KafkaProducer[String, String](props)
  
  // 开始时间
  val preTime = System.currentTimeMillis()
  
  // 从 taptap 上爬取用户评论数据(game_id, reviews)
  val crawlerData = Crawler.crawlData(pageNumPerGame)
  // 遍历各个游戏评论数据，发布评论数据到 Kafka
  var events = 0 // 评论计数
  for (entry <- crawlerData) {
    val (game_id, reviews) = entry
    reviews.foreach(review => {
      // 将评论数据以 UTF8 格式编码后解码，防止中文乱码
      val reviewUtf8 = new String(review.getBytes("UTF-8"), "UTF-8")
      // 创建消息，并以Json字符串形式编码发布
      val data = new ProducerRecord[String, String](topic, game_id.toString(),
          Map("gameId" -> game_id.toString(), "review" -> reviewUtf8).toJson.toString)
      // 异步向Kafka发送记录
      producer.send(data, new Callback() {
          //实现发送完成后的回调方法
          override def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
            if(e != null) {
              e.printStackTrace();
            } else {
              println("The offset of the record we just sent is: " + metadata.offset());
            }
          }
      })
      events += 1
    })
  }
  
  println("sent per second: " + events * 1000 / (System.currentTimeMillis() - preTime))
  producer.close()
}