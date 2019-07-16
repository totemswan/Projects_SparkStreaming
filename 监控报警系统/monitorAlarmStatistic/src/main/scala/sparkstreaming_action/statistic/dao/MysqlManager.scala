package sparkstreaming_action.statistic.dao

import org.apache.log4j.LogManager
import com.mchange.v2.c3p0.ComboPooledDataSource
import sparkstreaming_action.statistic.util.Conf
import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement

/**
 *  Mysql连接池类（c3p0）
 */
class MysqlPool extends Serializable {
  
  @transient lazy val log = LogManager.getLogger(this.getClass)
  
  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
  private val conf = Conf.mysqlConfig
  
  try {
    cpds.setJdbcUrl(conf.get("url")
        .getOrElse("jdbc:mysql://master:3306/spark?useUnicode=true&characterEncoding=UTF-8"))
    cpds.setDriverClass("com.mysql.jdbc.Driver")
    cpds.setUser(conf.get("username").getOrElse("hadoop"))
    cpds.setPassword(conf.get("password").getOrElse("123456"))
    cpds.setInitialPoolSize(3) // 初始连接数
    cpds.setMaxPoolSize(Conf.maxPoolSize) // 连接池保留的最大连接数
    cpds.setMinPoolSize(Conf.minPoolSize) // 连接池保留的最小连接数
    cpds.setAcquireIncrement(5) // 连接数递增步长
    cpds.setMaxStatements(180) // 最大缓存语句数
    /** 最大空闲时间：
     *  若25000秒内未使用则连接被丢弃；
     *  若为0，则永久不丢弃。
     *  Default: 0
     */
    cpds.setMaxIdleTime(25000)
    // 检测连接查询（前提是表需要存在）
    cpds.setPreferredTestQuery("select id from word_count_20190709 where id = 1")
    //每18000秒（5h）检查连接池中的所有空闲连接，Default: 0
    cpds.setIdleConnectionTestPeriod(18000)
  } catch {
    case e: Exception =>
      log.error("[MysqlPoolError]", e)
  }
  
  // 获取连接
  def getConnection: Connection = {
    try {
      return cpds.getConnection()
    } catch {
      case e: Exception =>
        log.error("[MysqlPool_getConnectionError]", e)
        null
    }
  }
  
  // 关闭连接
  def closeConnection(connection: Connection): Unit = {
    if (connection != null) {
      try {
        connection.close()
      } catch {
        case e: SQLException => 
          log.warn("[MysqlPool_closeConnectionError]", e)
      }
    }
  }
  
  // 关闭语句
  def closeStatement(statement: Statement): Unit = {
    if (statement != null) {
      try {
        statement.close()
      } catch {
        case e: SQLException => 
          log.warn("[MysqlPool_closeStatementError]", e)
      }
    }
  }
  
}

// 连接池单例
object MysqlManager {
  @volatile private var mysqlPool: MysqlPool = _
  def getlPool: MysqlPool = {
    if (mysqlPool == null) {
      synchronized {
        if (mysqlPool == null) {
          mysqlPool = new MysqlPool
        }
      }
    }
    mysqlPool
  }
}