package sparkstreaming_action.crawler.main

import org.apache.log4j.LogManager
import sparkstreaming_action.crawler.dao.MysqlManager
import java.sql.Connection
import java.sql.Statement
import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import scala.io.Source
import kantan.xpath._
import kantan.xpath.implicits._
import kantan.xpath.nekohtml._

/**
 *  引用包：
 *  kantan.xpath 解析包：https://nrinaudo.github.io/kantan.xpath/basics.html
 */

/** 
 *  游戏评论数据爬虫
 */
object Crawler {
  
  @transient lazy val log = LogManager.getLogger(this.getClass)
  
  /** 
   *  加载游戏数据库
   *  @return Map 游戏数据集合(game_id, game_name)
   */
  def getGames: Map[Int, String] = {
    // 加载开始执行时间
    val preTime = System.currentTimeMillis()
    // 查询语句：游戏库（目前只关注中文渠道language=zh-cn)
    val sql = "select * from games"
    // mysql 连接
    var conn: Connection = null
    // mysql 语句
    var statement: Statement = null
    try {
      // 获取连接
      conn = MysqlManager.getlPool.getConnection
      // 获取语句
      statement = conn.createStatement()
      // 执行sql语句
      val rs = statement.executeQuery(sql)
      // 提取返回结果
      val games = Map[Int, String]()
      while (rs.next()) {
        games += (rs.getInt("game_id") -> rs.getString("game_name"))
      }
      log.warn("[loadGamesSuccess] load entities from db "
          + s"count: ${games.size}\t"
          + s"time elapsed: ${System.currentTimeMillis() - preTime}")
      games
    } catch {
      case e: Exception =>
        log.error("[getGames_loadError]", e)
        Map[Int, String]()
    } finally {
      MysqlManager.getlPool.closeConnection(conn)
      MysqlManager.getlPool.closeStatement(statement)
    }
  }
  
  /** 
   *  从taptap上爬取用户评论数据
   *  @param pageNum 爬取页面数目
   *  @return Map 返回评论数据(game_id, List(reviews))
   */
  def crawlData(pageNum: Int): Map[Int, List[String]] = {
    // 加载游戏数据
    val games = getGames
    println("games: " + games)
    // 按游戏ID，依次爬去用户评论数据
    val data = Map[Int, List[String]]()
    games.foreach(game => {
      val (game_id, game_name) = game
      // 评论数据缓存
      val reviews = ListBuffer[String]()
      // 遍历指定页数
      for (page <- 0 until pageNum) {
        // 爬取地址
        val url = s"https://www.taptap.com/app/$game_id/review?order=default&page=$page#review-list"
        println("craw url: " + url)
        // 爬取链接HTML内容（也可用HttpRequest请求）
        val html = Source.fromURL(url).mkString
        // 利用XPath 正则解析出HTML中的评论数据 <div class='item-text-body'><p>reviews</div>
        val rs = html.evalXPath[List[String]](xp"//div[@class='item-text-body']/p")
        if (rs.isRight) {
//          println(rs.right.get)
          reviews ++= rs.right.get
        }
      }
      log.info(s"$game_name craw data size: ${reviews.size}")
      // 添加当前游戏及其评论数据到集合
      data += (game_id -> reviews.toList)
    })
    data
  }
  
  // 测试
  def main(args: Array[String]) {
    // 爬取一页数据测试
    val reviews = crawlData(1)
    println(reviews)
    
    // 测试 nekohtml 解析
    val html = "<div class='item-text-body'><p>123</div><div class='other-body'><p>other</div>"
    val data1 = html.evalXPath[List[String]](xp"//div").right.get
    println(data1)
    val data2 = html.evalXPath[List[String]](xp"//div[@class='item-text-body']").right.get
    println(data2)
    val data3 = html.evalXPath[List[String]](xp"//div[@class='item-text-body']/p").right.get
    println(data3)
  }
}