package sparkstreaming_action.count.util;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

/**
 * Kafka工具类
 * @author wayne
 *
 */
public class KafkaUtils {

	static Logger log = Logger.getLogger(KafkaUtils.class);
	
	private static KafkaUtils instance = new KafkaUtils();
	private KafkaProducer<String, byte[]> producer = null;
	private KafkaConsumer<String, String> consumer = null;
	private String producerTopic = "";
	private boolean autoCommit = false;	// 手动同步offset到Kafka
	private Set<TopicPartition> assignedPartitions = null; // 固定分配的分区
	
	public KafkaUtils() {
	}
	
	public static KafkaUtils getInstance() {
		return instance;
	}
	
	public KafkaProducer<String, byte[]> getProducer() {
		return producer;
	}
	
	public KafkaConsumer<String, String> getConsumer() {
		return consumer;
	}
	
	public String getProducerTopic() {
		return producerTopic;
	}
	
	public boolean initialize() {
		try {
			// 消费者配置
			Properties propsConsumer = new Properties();
			propsConsumer.put("bootstrap.servers", ConfigUtils.getConfig("bootstrap.servers"));
			propsConsumer.put("group.id", ConfigUtils.getConfig("group.id"));
			propsConsumer.put("enable.auto.commit", autoCommit);
			propsConsumer.put("auto.offset.reset", ConfigUtils.getConfig("auto.offset.reset"));
			propsConsumer.put("session.timeout.ms", "35000");
			propsConsumer.put("max.partition.fetch.bytes", 64 * 1024 * 1024); // 64MB
			propsConsumer.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			propsConsumer.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			propsConsumer.put("max.poll.records", ConfigUtils.getConfig("max.poll.records")); // 最大拉取记录数
			// 初始化消费者
			consumer = new KafkaConsumer<String, String>(propsConsumer);
			
			// manually assign partitions. when assign partitions manually,
			// "subscribe" should not be used
			// 获取主题
			String topic = ConfigUtils.getConfig("consumer.topic");
			// 获取主题的所有分区
			List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
			// 构建主题分区集合 List<TopicPartition>
			LinkedList<TopicPartition> topicPartitions = new LinkedList<TopicPartition>();
			for (PartitionInfo info : partitionInfos) {
				log.info("topic has partition: " + info.partition());
				TopicPartition topicPartition = new TopicPartition(topic, info.partition());
				topicPartitions.add(topicPartition);
			}
			/** 为消费者手动分配分区
			 *    注：consumer.subscribe() 为动态分配分区
			 *        subscribe() 和 assign() 不能同时使用
			 */
			consumer.assign(topicPartitions);
			
			/** 获取该消费者分配到的分区信息
			 *    注：如果是 assign() 分配方式，那么 assignment() 返回结果是和手动分配一样的分区
			 *        如果是 subscribe() 订阅方式，那么 assignment() 返回结果是当前分配到的分区
			 */
			assignedPartitions = consumer.assignment(); // 与手动分配的分区相同
			// log initial partition positions
			log.info("Initial partition positions:");
			logPosition(false);
			
			/** 按分区读取策略（从配置文件中读取），日志打印分区offsets信息
			 *    注：1. 从分区的起始开始读； 2. 从分区的末尾开始读
			 */
			if (ConfigUtils.getBooleanValue("frombegin")) { // if seek to begin
				consumer.seekToBeginning(topicPartitions);
				log.info("Seek to beginning is set, after seek to beginning, now partition positions.");
				logPosition(false);
			}
			if (ConfigUtils.getBooleanValue("fromend")) { // if seek to end
				consumer.seekToEnd(topicPartitions);
				log.info("Seek to end is set, after seek to end, now partition positions:");
				logPosition(false);
			}
			
		} catch (Exception e) {
			log.error("initial KafkaUtils fails, e: " + ExceptionUtils.getStackTrace(e));
			return false;
		}
		return true;
	}
	
	/**
	 * 手动提交消费记录的分区偏移量offsets
	 * @param fetchedRecords
	 * @param needLog
	 */
	public void tryCommit(ConsumerRecords<String, String> fetchedRecords, boolean needLog) {
		// only if the poll operation get messages, commit offsets
		if (fetchedRecords.count() > 0 && !autoCommit) {
			// 手动提交offsets到Kafka，同步执行，会阻塞等待直到执行完成
			consumer.commitSync();
			if (needLog) {
				log.info("=== After commitSync, now partition positions:");
				logPosition(false);
			}
		}
	}
	
	/** 
	 * 日志输出消费分区信息offsets
	 * @param debug 日志级别： debug or info
	 */
	private void logPosition(boolean debug) {
		for (TopicPartition assignedPartition : assignedPartitions) {
			if (debug) {
				/**
				 *  consumer.position() 返回下一条记录的offset
				 */
				log.debug(String.format("partition %d position: %d", assignedPartition.partition(),
						consumer.position(assignedPartition)));
			} else {
				log.info(String.format("partiton %d position: %d", assignedPartition.partition(),
						consumer.position(assignedPartition)));
			}
		}
	}
}
