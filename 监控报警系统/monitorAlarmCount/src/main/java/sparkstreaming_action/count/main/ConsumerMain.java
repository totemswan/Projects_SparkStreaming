package sparkstreaming_action.count.main;

import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import com.google.gson.Gson;

import sparkstreaming_action.count.entity.Record;
import sparkstreaming_action.count.service.AlarmLayer;
import sparkstreaming_action.count.service.CountLayer;
import sparkstreaming_action.count.service.FilterLayer;
import sparkstreaming_action.count.util.CommonUtils;
import sparkstreaming_action.count.util.ConfigUtils;
import sparkstreaming_action.count.util.KafkaUtils;
import sparkstreaming_action.count.util.MysqlUtils;
import sparkstreaming_action.count.util.TimeUtils;
import sparkstreaming_action.count.util.TrashFilterUtils;

public class ConsumerMain {

	private static Logger log = Logger.getLogger(ConsumerMain.class);
	
	private static Gson gson = new Gson();
	private Long nextReloadTime = 0L; // 规则库重载时间间隔
	private Long lastRulesUpdateTime = 0L; // 规则库上次更新时间
	private Long lastGamesUpdateTime = 0L; // 监控游戏上次更新时间
	private Long kafkaLogTimes = 0L; // Kafka 消费次数日志统计
	
	// 过滤层
	private FilterLayer filterLayer;
	// 统计层
	private CountLayer countLayer;
	// 报警层
	private AlarmLayer alarmLayer;
	
	public ConsumerMain(long beginTime) {
		filterLayer = new FilterLayer();
		countLayer = new CountLayer();
		alarmLayer = new AlarmLayer(countLayer);
		nextReloadTime = TimeUtils.currentTimeSeconds() + ConfigUtils.getIntValue("reload_interval");
		lastRulesUpdateTime = MysqlUtils.getUpdateTime("rules");
		lastGamesUpdateTime = MysqlUtils.getUpdateTime("monitor_games");
	}
	
	/**
	 * 
	 */
	public void run() {
		// Kafka 接收数据层
		KafkaUtils kafkaUtils = KafkaUtils.getInstance();
		// Kafka 初始化
		if (!kafkaUtils.initialize()) {
			log.error("kafka init error! exit!");
			System.exit(-1);
		}
		// 获取 Kafka 消费者
		KafkaConsumer<String, String> consumer = kafkaUtils.getConsumer();
		long count = 0;
		// 消费者任务
		while (true) {
			try {
				// 拉取消费记录，没有记录可拉取时，最长等待200ms
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
				// 遍历消息记录
				for (ConsumerRecord<String, String> record : records) {
					if (count++ % 50 == 0) {
						log.warn("[CurDataCount] count: " + count);
					}
					// 解析Json格式消息为Record对象
					Record r = gson.fromJson(record.value(), Record.class);
					log.info("[Record info: ] " + r);
					// 被过滤掉的消息不做处理
					if (filterLayer.filter(r)) {
						continue;
					}
					// 未被过滤掉的消息，添加至统计层处理
					countLayer.addRecord(r);
				}
				// 运行报警层算法
				alarmLayer.alarm();
				
				// 一批消息成功消费后，手动提交此批消息的偏移offsets
				if (kafkaLogTimes++ % 10 == 0) { // 每 10 次消费 输出一次日志信息
					kafkaUtils.tryCommit(records, true);
				} else {
					kafkaUtils.tryCommit(records, false);
				}
				
				// 阶段性重新加载相关数据
				if (nextReloadTime <= TimeUtils.currentTimeSeconds()) {
					// 规则库、监控游戏库更改，则重新加载
					long rulesUpdateTime = MysqlUtils.getUpdateTime("rules");
					long gamesUpdateTime = MysqlUtils.getUpdateTime("monitor_games");
					if (rulesUpdateTime != lastRulesUpdateTime || gamesUpdateTime != lastGamesUpdateTime) {
						log.warn("rules or games changed!");
						countLayer.reload();
						lastRulesUpdateTime = rulesUpdateTime;
						lastGamesUpdateTime = gamesUpdateTime;
					}
					// 垃圾过滤规则文件若有更改，则重新加载
					if (CommonUtils.isFileChange("patterns_appstore.txt", "patterns_forum.txt")) {
						TrashFilterUtils.reload();
					}
					// 下次重载时间计算：增加n个步长
					while (nextReloadTime <= TimeUtils.currentTimeSeconds()) {
						nextReloadTime += ConfigUtils.getIntValue("reload_interval");
					}
				}
			} catch (Exception e) {
				log.warn("main error: " + CommonUtils.getStackTrace(e));
			}
		}
	}
	
	// 程序启动入口
	public static void main(String[] args) {
		final ConsumerMain consumerMain = new ConsumerMain(TimeUtils.currentTimeSeconds());
		log.warn("All things init done!");
		consumerMain.run();
	}
	
}
