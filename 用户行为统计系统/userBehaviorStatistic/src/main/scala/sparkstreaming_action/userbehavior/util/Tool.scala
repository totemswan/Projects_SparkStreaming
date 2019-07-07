package sparkstreaming_action.userbehavior.util

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.LogManager

/**
 *  工具单例
 */
object Tool extends Serializable{
  
  @transient lazy val log = LogManager.getLogger(this.getClass)
  
  /** 解码：将字节数组转成指定类型数据
   *  @param byteArr: Array[Byte] 字节数组
   *  @param doRead: Function 自定义解码方式
   *  @return Option[T] 返回解码后的Option包装的指定类型
   */
  def decode[T](byteArr: Array[Byte])(doRead: (DataInputStream) => T): Option[T] = {
    val bais = new ByteArrayInputStream(byteArr)
    val dis = new DataInputStream(bais)
    try {
      // 执行自定义输出编码方式
      Option(doRead(dis))
    } catch {
      case e: Exception =>
        log.error("[decodeError]", e)
        /**
         *  返回值 T 需要使用 Option 包装成 Option[T]
         *  否则如果返回类型只是 T ，则此处无法写 null 返回
         */
        None
    } finally {
      dis.close()
      bais.close()
    }
  }
  
  /** 编码：将指定类型数据转成字节数组
   *  @param any: Any 任意数据
   *  @param doWrite: Function 自定义编码方式
   *  @return Option[Array[Byte]] 返回编码后的字节数组（Option包装）
   */
  def encode(any: Any)(doWrite: (DataOutputStream, Any) => Unit): Option[Array[Byte]] = {
    // 创建数据转字节数据的输出流
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    try {
      // 执行自定义输出编码方式
      doWrite(dos, any)
      // 获取输出流中的转码后字节数组
      Option(baos.toByteArray())
    } catch {
      case e: Exception =>
        log.error("[encodeError]", e)
      None
    } finally {
      dos.close()
      baos.close()
    }
  }
  
  /**
   *  判断时间戳是否已过期
   *    即：该时间戳是否不在当前时间以前的一段范围内，以表示是否过期
   *  @param timestamp: Long 时间戳，单位：s
   *  @param duration: Long 持续时间（有效时间长度），单位：s
   *  @return Boolean true|false
   */
  def isInvalidate(timestamp: Long, duration: Long): Boolean = {
    // 阈值：持续时间相对于当前时间（上限）的下限阈值
    val threshold = System.currentTimeMillis() / 1000 - duration
    // 判断 timestamp 是否不在 持续时间内 [threshold, currentTime]，即时间是否已过期
    timestamp < threshold
  }
  
}