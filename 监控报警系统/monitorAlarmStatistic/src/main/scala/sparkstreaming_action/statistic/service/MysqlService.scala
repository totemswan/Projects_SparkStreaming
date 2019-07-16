package sparkstreaming_action.statistic.service

import org.apache.log4j.LogManager
import sparkstreaming_action.statistic.entity.MonitorGame
import sparkstreaming_action.statistic.dao.MysqlManager
import java.sql.Connection
import java.sql.Statement
import scala.collection.mutable.Map

/**
 *  Mysql 服务
 */
object MysqlService extends Serializable{
  
  @transient lazy val log = LogManager.getLogger(this.getClass)
  
  /** 
   *  加载监控游戏库
   *  @return 
   */
  def getMonitorGames: Map[Int, MonitorGame] = {
    // 加载开始执行时间
    val preTime = System.currentTimeMillis()
    // 查询语句：监控游戏库
    val sql = "select * from monitor_games"
    // mysql 连接
    var conn: Connection = null
    // mysql 语句
    var statement: Statement = null
    try {
      // 获取连接
      conn = MysqlManager.getlPool.getConnection
      // 获取语句
      statement = conn.createStatement()
      // 执行sql语句，得到查询结果集
      val rs = statement.executeQuery(sql)
      // 提取返回结果
      val games = Map[Int, MonitorGame]()
      while (rs.next()) {
        games += (rs.getInt("game_id") -> new MonitorGame(
            rs.getInt("game_id"), 
            rs.getString("game_name")))
      }
      log.warn("[loadMonitorGamesSuccess] load entities from db "
          + s"count: ${games.size}\t"
          + s"time elapsed: ${System.currentTimeMillis() - preTime}")
      games
    } catch {
      case e: Exception =>
        log.error("[getGames_loadError]", e)
        Map[Int, MonitorGame]()
    } finally {
      MysqlManager.getlPool.closeConnection(conn)
      MysqlManager.getlPool.closeStatement(statement)
    }
  }
  
}