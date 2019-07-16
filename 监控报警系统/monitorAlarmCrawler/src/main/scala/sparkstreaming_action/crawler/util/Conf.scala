package sparkstreaming_action.crawler.util

/** 
 *  配置信息
 */
object Conf extends Serializable {
  
  // mysql configuration
  val mysqlConfig = Map(
      "url" -> "jdbc:mysql://master:3306/spark?characterEncoding=UTF-8",
      "username" -> "hadoop",
      "password" -> "123456")
  val maxPoolSize = 5
  val minPoolSize = 2
}