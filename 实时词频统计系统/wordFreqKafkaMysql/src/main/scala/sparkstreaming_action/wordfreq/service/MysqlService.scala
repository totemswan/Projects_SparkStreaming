package sparkstreaming_action.wordfreq.service

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashSet

import sparkstreaming_action.wordfreq.dao.MysqlManager
import sparkstreaming_action.wordfreq.util.TimeParse

/** 
 *  Mysql数据库服务
 */
object MysqlService extends Serializable {
  
  @transient lazy val log = LogManager.getLogger(this.getClass)
  
  /** 保存
   *  按RDD分区批量执行插入语句
   */
  def save(rdd: RDD[(String, Int)]): Unit = {
    if (!rdd.isEmpty()) {
      rdd.foreachPartition(partition => {
        // 每个分区开始执行时间
        val preTime = System.currentTimeMillis()
        // 从连接池获取数据库连接
        val conn = MysqlManager.getMysqlPool.getConnection
        // 获取语句
        val statement = conn.createStatement()
        try {
          conn.setAutoCommit(false) // 手动提交事务
          partition.foreach((record: (String, Int)) => {
            log.info(">>>>>>" + record)
            // 创建时间
            val createTime = System.currentTimeMillis()
            /** 按 年月 创建分表word_count_yyyyMM 
             *  	对 (word, date) 组合做唯一性校验
             *  注1：scala中字符串换行书写时，需要把 "+" 号放在上一行的右侧
             *  		如：var sql = "abc" +
             *  						"def"
             *  				println(sql) // abcdef
             *  注2：为了观看方便，写在左侧，但又不支持，
             *  		 所以将字符串用括号套起来
             *  		如：var sql = ("abc"
             *  						+ "def")
             *  				println(sql) // abcdef
             */
            var sql = ("CREATE TABLE IF NOT EXISTS "
                + s"word_count_${TimeParse.timeStamp2String(createTime, "yyyyMM")} "
                + "(id int(11) NOT NULL AUTO_INCREMENT,"
                + " word varchar(64) NOT NULL,"
                + " count int(11) DEFAULT 0,"
                + " date date NOT NULL,"
                + " PRIMARY KEY (id),"
                + " UNIQUE KEY word (word, date)" 
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;")
            statement.addBatch(sql)
            /** 插入语句
             *  	对 (word, date) 组合出现重复（冲突）的插入，执行count值的累加更新
             */
            sql = ("insert into "
                + s"word_count_${TimeParse.timeStamp2String(createTime, "yyyyMM")} "
                + "(word, count, date) values ("
                + s"'${record._1}', ${record._2},"
                + s"'${TimeParse.timeStamp2String(createTime, "yyyy-MM-dd")}'"
                + ") on duplicate key update count=count+values(count);")
            statement.addBatch(sql)
            log.warn(s"[recordAddBatchSuccess] record: ${record._1}, ${record._2}")
          })
          // 执行批次
          statement.executeBatch()
          // 提交批次事务
          conn.commit()
          // 每个分区的批次执行总时间
          log.warn(s"[save_batchSaveSuccess] " 
              + s"time elapsed: ${System.currentTimeMillis() - preTime}")
        } catch {
          case e: Exception =>
            log.error("[save_batchSaveError]", e)
        } finally {
          statement.close()
          conn.close()
        }
      })
    }
  }
  
  /** 加载用户词典
   *  @return HashSet[String]
   */
  def getUserWords(): HashSet[String] = {
    // 开始时间
    val preTime = System.currentTimeMillis()
    // sql查询
    val sql = "select distinct(word) from user_words"
    // 从连接池获取Mysql数据库连接
    val conn = MysqlManager.getMysqlPool.getConnection
    // 获取语句
    val statement = conn.createStatement()
    try {
      // 执行查询，获取结果集（result set）
      val rs = statement.executeQuery(sql)
      // 存放结果词语
      val words = HashSet[String]()
      // 遍历结果集
      while (rs.next()) {
        words += rs.getString("word")
      }
      log.warn("[loadSuccess] load user_words from db " 
          + s"count: ${words.size}\t"
          + s"time elapsed: ${System.currentTimeMillis() - preTime}")
      words
    } catch {
      case e: Exception => 
        log.error("[loadError] error: ", e)
        null
    } finally {
      statement.close()
      conn.close()
    }
  }
}