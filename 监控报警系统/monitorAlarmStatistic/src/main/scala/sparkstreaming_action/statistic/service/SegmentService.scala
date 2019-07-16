package sparkstreaming_action.statistic.service

import org.apache.log4j.LogManager
import sparkstreaming_action.statistic.entity.MonitorGame

import spray.json._
import spray.json.DefaultJsonProtocol._

import com.huaban.analysis.jieba.JiebaSegmenter
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode

import scala.collection.mutable.MutableList
import scala.collection.JavaConversions
import scala.collection.mutable.Map


/** 
 *  引用包：
 *  spray-json包: https://github.com/spray/spray-json
 *  
 *  jieba分词源码及教程：https://github.com/fxsjy/jieba
 *  jieba分词Java版本：https://github.com/huaban/jieba-analysis
 */

/** 
 *  分词服务
 */
object SegmentService extends Serializable {
  
  @transient lazy val log = LogManager.getLogger(this.getClass)
  
  /** 
   *  将Json文本内容分词
   *  @param reviewJson 一条游戏评论数据的Json字符串，格式："""{ "gameId":2, "review":"我们是共产主义接班人" }"""
   *  @param monitorGames 待监控的游戏数据库
   *  @return 
   */
  def mapSegment(reviewJson: String, monitorGames: Map[Int, MonitorGame]): Option[(Int, String)] = {
    // 分词开始时间
    val preTime = System.currentTimeMillis()
    try {
      // 解析评论的Json字符串为Json对象，模式匹配关键字段
      reviewJson.parseJson.asJsObject.getFields("gameId", "review") match {
        // 匹配成功
        case Seq(JsString(gameId), JsString(review)) => {
            /** 1.如果该条评论的游戏不包含在监控游戏库中，忽略
             *  2.如果该条评论的游戏包含在监控游戏库中，进行分词
             */
            if (!monitorGames.contains(gameId.toInt)) {
              log.warn(s"[Segment_GameIgnored] no need to monitor gameId: ${gameId}")
              None
            } else if (review == null || "".equals(review.trim())) {
              log.warn(s"[Segment_ReviewEmpty] reviewJson: ${reviewJson}")
              None
            } else {
              try {
                // 获取监控游戏
                val game = monitorGames.get(gameId.toInt).get
                // 构造返回的Json结果
                val jsonObj = JsObject(
                    "gameId" -> JsNumber(game.gameId),
                    "gameName" -> JsString(game.gameName),
                    "review" -> JsString(review),
                    "reviewSeg" -> JsString(segment(format(review)))
                    )
                log.warn(s"[Segment_Success] gameId: ${game.gameId}\tgameName: ${game.gameName}\t"
                  + s"time elapsed: ${System.currentTimeMillis() - preTime}\t"
                  + s"MonitorGames count: ${monitorGames.size}\n")
                // 返回分词结果Some包装
                Some((game.gameId, jsonObj.toString))
              } catch {
                case e: Exception => 
                  log.warn(s"[Segment_Error] mapSegment error\treviewJson string: ${reviewJson}"
                    + s"review: ${review}", e)
                  None
              }
            }
          }
        // 匹配失败
        case _ => {
          log.warn(s"[Segment_MatchFailed] reviewJson parse match failed\treviewJson string: ${reviewJson}")
          None
        }
      }
    } catch {
      case e: Exception => 
        log.warn(s"[Segment_JsonParseError] mapSegment error\treviewJson string: ${reviewJson}", e)
        None
    }
  }
  
  /** 
   *  格式化字符串
   *  @param s 字符串
   *  @return 格式化后的字符串（替换Tab制表符为空格）
   */
  def format(s: String): String = s.replace("\t", " ")
  
  /** 
   *  分词
   *  @param review 评论文本字符串
   *  @return 分词结果字符串
   */
  def segment(review: String): String = {
    /** 
     *  分词模式：
     *    Search模式：用于对用户查询词分词
     *    Index模式：用于对索引文档分词
     */
    val seg = new JiebaSegmenter
    var st = seg.process(review, SegMode.SEARCH)
    val words = MutableList[String]()
    for (t <- JavaConversions.asScalaBuffer(st)) { // 使用Java->scala隐式转换
      words += t.word
    }
    words.mkString("\t")
  }
  
  // 测试
  def main(args: Array[String]) = {
    val result = segment("我们是共产主义接班人 sefef dfd d <script>ddddd dd </script>")
    println(result)
  }
  
}