package sparkstreaming_action.statistic.util

import org.apache.spark.streaming.StreamingContext
import scala.reflect.ClassTag
import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import org.apache.spark.broadcast.Broadcast

/**
 * 广播变量包装器
 * （支持运行时动态更新）
 * @param ssc: StreamingContext 流式上下文
 * @param _v: T 待广播数据
 */
case class BroadcastWrapper[T: ClassTag](
    @transient private val ssc: StreamingContext,
    @transient private val _v: T) {
  
  // 广播变量
  @transient private var v = ssc.sparkContext.broadcast(_v)
  
  /** 更新广播变量
   *  @param newValue: T 新的待广播数据
   *  @param blocking: Boolean 是否阻塞广播变量的使用，直到广播变量重新广播完成
   */
  def update(newValue: T, blocking: Boolean = false): Unit = {
    v.unpersist(blocking)
    v = ssc.sparkContext.broadcast(newValue)
  }
  
  // 广播变量的数据
  def value: T = v.value
  
  // 序列化广播变量对象
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(v)
  }
  
  // 反序列化广播变量对象
  private def readObject(in: ObjectInputStream): Unit = {
    v = in.readObject().asInstanceOf[Broadcast[T]]
  }
  
}