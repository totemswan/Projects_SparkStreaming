package sparkstreaming_action.count.service;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import sparkstreaming_action.count.entity.Alarm;
import sparkstreaming_action.count.entity.Rule;
import sparkstreaming_action.count.util.MysqlUtils;

/**
 * 扫描统计层和报警规则层，发出报警
 * @author wayne
 *
 */
public class AlarmLayer {

	private static Logger log = Logger.getLogger(AlarmLayer.class);
	
	private CountLayer countLayer;
	
	public AlarmLayer(CountLayer countLayer) {
		this.countLayer = countLayer;
		log.warn("AlarmLayer init done!");
	}
	
	/**
	 * 根据统计层和规则层进行报警
	 * @return
	 */
	public void alarm() {
		// 遍历报警规则游戏
		for (Entry<Integer, Map<Integer, Map<String, Integer>>> grwc : countLayer.gameRuleWordCount.entrySet()) {
			// 获取规则游戏ID
			int gameId = grwc.getKey();
			// 遍历规则库
			for (Entry<Integer, Map<String, Integer>> rwc : grwc.getValue().entrySet()) {
				// 获取规则ID
				int ruleId = rwc.getKey();
				// 获取规则库数据
				Rule rule = countLayer.idRule.get(ruleId);
				/** 报警算法：
				 *    统计该规则库的关键词词频，若达到阈值则报警
				 *    统计类型type：0|按词平均值；1|按词之和
				 */
				double sum = 0, count = 0;
				for (Entry<String, Integer> wc : rwc.getValue().entrySet()) {
					sum += wc.getValue();
					count += 1;
				}
				// 0|按词平均值；1|按词之和
				if (rule.type == 0) {
					sum /= count;
				}
				// 超过词频限制，进行报警
				if (sum >= rule.threshold) {
					// 构建报警对象
					Alarm alarm = new Alarm();
					alarm.game_id = gameId;
					alarm.game_name = rule.game_name;
					alarm.rule_id = ruleId;
					alarm.rule_name = rule.rule_name;
					alarm.has_sent = 0;
					alarm.is_problem = -1;
					alarm.words = rule.words;
					alarm.words_freq = map2Str(rwc.getValue());
					// 报警信息插入至数据库
					MysqlUtils.insert("alarms", alarm);
					log.warn(alarm.toString());
					// 每报警一次，则重新统计，更新词频 归 0
					for (String w : rwc.getValue().keySet()) {
						rwc.getValue().put(w, 0);
					}
				}
			}
		}
	}
	
	/**
	 * 将Map集合数据转换到字符串
	 * @param m
	 * @return
	 */
	public String map2Str(Map<String, Integer> m) {
		StringBuilder sb = new StringBuilder();
		for (Entry<String, Integer> e : m.entrySet()) {
			sb.append(String.format("%s:%d", e.getKey(), e.getValue()));
		}
		return sb.toString();
	}
	
}
