package sparkstreaming_action.statistic.service

import spray.json._
import sparkstreaming_action.statistic.entity.Record

/** 
 *  spray-json包 源码及教程: https://github.com/spray/spray-json
 */

/** 
 *  Json解析服务
 */
object MyJsonProtocol extends DefaultJsonProtocol{
  implicit val docFormat = jsonFormat2(Record)
}

/** 
 *  类与Json字符串互转
 */
object JsonParse {
  
  import MyJsonProtocol._
  
  def record2Json(doc: Record): String = {
    doc.toJson.toString()
  }
  
  def json2Record(json: String): Record = {
    json.parseJson.convertTo[Record]
  }
}

