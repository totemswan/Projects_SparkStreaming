package sparkstreaming_action.wordfreq.service

import org.apache.log4j.LogManager
import scala.collection.mutable.HashSet
import scala.collection.mutable.Map
import scalaj.http.Http
import spray.json._
import spray.json.DefaultJsonProtocol._
import sparkstreaming_action.wordfreq.util.Conf

/** 引用包使用说明地址：
 * spray-json包: https://github.com/spray/spray-json
 * scalaj-http包: https://index.scala-lang.org/scalaj/scalaj-http/scalaj-http
 */

/**
 * 分词服务
 */
object SegmentService extends Serializable {
  
  @transient lazy val log = LogManager.getLogger(this.getClass)
  
  /** 指定词 统计
   *  @param record: String 待分词记录
   *  @param wordDictionary: HashSet[String] 指定词语的词典
   *  @return
   */
  def mapSegment(record: String, wordDictionary: HashSet[String]): Map[String, Int] = {
    // 分词开始时间
    val preTime = System.currentTimeMillis()
    // 指定词 统计集合
    val wordCount = Map[String, Int]()
    
    if (record == null || record.isEmpty()) {
      log.warn(s"record is empty.")
      wordCount
    } else {
      // 分词服务地址
      val postUrl = Conf.segmentorHost + "/token/"
      try {
        // 对记录分词
        val wordsSet = retry(3)(segment(postUrl, record))
        log.warn(s"[mapSegmentSuccess] record: ${record}\t"
            + s"time elapsed: ${System.currentTimeMillis() - preTime}")
        // 按词库指定词 统计
        wordDictionary.foreach(word => {
          if (wordsSet.contains(word))
            wordCount += word -> 1
        })
        wordCount
      } catch {
        case e: Exception => 
          log.warn("[mapSegmentApiError] mapSegment error\t"
              + s"postUrl: ${postUrl}${record}", e)
              wordCount
      }
    }
    
  }
  
  /** 分词
   *  @param url: String 分词服务地址
   *  @param content: String 待分词内容
   *  @return HashSet[String] 词语集合
   */
  def segment(url: String, content: String): HashSet[String] = {
    // 请求开始时间
    val timer = System.currentTimeMillis()
    // 发送请求，等待响应
    var response = Http(url + content).asString
    // 计算响应时间（耗时时间）
    val dur = System.currentTimeMillis() - timer
    
    if (dur > 20) // 输出耗时较长的请求
      log.warn(s"[longVisit]>>>>>> api: ${url}${content}\ttimer: ${dur}")
    
    // 分词词语结果集
    val words = HashSet[String]()
    response.code match {
      // 匹配响应成功信号
      case 200 => {
        // parseJson 将Json字符串转成Json语法树节点（Abstract Syntax Tree(AST) node）
        // asJsObject 将Json AST 转成 Json对象，便于面向对象操作
        response.body.parseJson.asJsObject.getFields("ret", "msg", "terms") match {
          /** 匹配响应参数列表
           *  Seq() 默认实现了 List（广义表）
           *  	val list = Seq(1,"a",3)
           *  	println(list.head) //1
           *  	println(list.tail) //List(a, 3)
           *  
           *  JsNumber/JsString 继承自 JsValue，包装名/值对
           */
          case Seq(JsNumber(ret), JsString(msg), JsString(terms)) => {
            if (ret.toInt != 0) { // 分词失败
              log.error("[segmentRetError] visit api: " 
                  + s"${url}?content=${content}\tsegment error: ${msg}")
            } else { // 分词成功
              val tokens = terms.split(" ")
              tokens.foreach(token => {
                words += token // 词语插入集合
              })
            }
          }
          // 匹配失败，返回空集合
          case _ => words
        }
        words
      }
      // 匹配响应异常信号
      case _ => {
        log.error("[segmentResponseError] visit api: "
            + s"${url}${content}\tresponse code: ${response.code}")
        words
      }
    }
      
  }
  
  /** HTTP请求重试
   *  出错原因：网络出错
   *  尝试 n 次，若依然失败，则抛出异常
   *  @param n 尝试次数
   *  @param fn 执行函数体
   *  @return
   */
  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): T = {  // 使用了函数柯里化
    util.Try { fn } match {
      case util.Success(x) => x
      case _ if n > 1 => {
        log.warn(s"[retry ${n}]")
        retry(n - 1)(fn)
      }
      case util.Failure(e) => {
        log.error(s"[segError] API retry 3 times fail!", e)
        throw e 
      }
    }
  }
  
}