package sparkstreaming_action.count.service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import sparkstreaming_action.count.dao.RulesDao;
import sparkstreaming_action.count.entity.Record;
import sparkstreaming_action.count.entity.Rule;
import sparkstreaming_action.count.util.ConfigUtils;

/**
 * 统计层数据结构
 *   按游戏规则库的关键词统计口径统计词频
 * @author wayne
 *
 */
public class CountLayer {

	private static Logger log = Logger.getLogger(CountLayer.class);
	
	/**
	 * game rule word 三者关系
	 * game 1->n rule 1->n word
	 */
	// 规则库 解析数据 的数据结构：[gameId -> [ruleId -> [word -> count]]]
	// 以gameId, ruleId, word 建立关键词的Hash索引
	public Map<Integer, Map<Integer, Map<String, Integer>>> gameRuleWordCount;
	
	// 规则库 原始数据 的数据结构：[ruleId -> rule]
	// 以ruleId建立规则库的Hash索引
	public Map<Integer, Rule> idRule;
	
	public CountLayer() {
		reload();
		log.warn("CountLayer init done!");
	}
	
	/**
	 * 将一条记录按照createTime统计
	 * @param record
	 */
	public void addRecord(Record record) {
		// 获取评论数据所属游戏ID
		int gameId = record.gameId;
		// 查询该游戏的规则库，不存在则不做处理
		if (!gameRuleWordCount.containsKey(gameId)) {
			log.error("GameRuleWordCount don't contain gameId: " + gameId);
			return;
		}
		// 若存在则遍历该游戏的规则库
		for (Entry<Integer, Map<String, Integer>> ruleWord : gameRuleWordCount.get(gameId).entrySet()) {
			int ruleId = ruleWord.getKey();
			// 遍历每个规则库的统计关键词
			for (Entry<String, Integer> wordCount : ruleWord.getValue().entrySet()) {
				String word = wordCount.getKey();
				// 若评论中包含统计的关键词，则关键词计数 +1
				if (contains(record, word)) {
					gameRuleWordCount.get(gameId).get(ruleId).put(word, wordCount.getValue() + 1);
				}
			}
		}
	}
	
	/**
	 * 重新加载规则库和监控游戏库
	 */
	public void reload() {
		// 数据库加载规则库表数据
		List<Rule> rules = RulesDao.getGameRules();
		// 定义空集合
		Map<Integer, Map<Integer, Map<String, Integer>>> newGameRuleWordCount = new HashMap<Integer, Map<Integer, Map<String, Integer>>>();
		idRule = new HashMap<Integer, Rule>();
		// 遍历规则库
		for (Rule rule : rules) {
			// 1. 建立规则库原始数据索引，全量数据覆盖
			idRule.put(rule.rule_id, rule);
			
			// 2.1. 建立规则库解析数据的gameId索引
			if (!newGameRuleWordCount.containsKey(rule.game_id)) {
				newGameRuleWordCount.put(rule.game_id, new HashMap<Integer, Map<String, Integer>>());
			}
			// 2.2. 建立规则库解析数据的ruleId索引
			if (!newGameRuleWordCount.get(rule.game_id).containsKey(rule.rule_id)) {
				newGameRuleWordCount.get(rule.game_id).put(rule.rule_id, new HashMap<String, Integer>());
			}
			// 2.3. 建立规则库解析数据的word索引
			for (String word : rule.words.split(" ")) {
				/** 支持增量数据加入：
				 *     有原数据则合并，新数据则插入，丢弃新数据中不存在的原数据
				 *     保持与重新加载的规则库关键词统计口径相同
				 */
				if (gameRuleWordCount != null && gameRuleWordCount.containsKey(rule.game_id)
						&& gameRuleWordCount.get(rule.game_id).containsKey(rule.rule_id)
						&& gameRuleWordCount.get(rule.game_id).get(rule.rule_id).containsKey(word)) {
					newGameRuleWordCount.get(rule.game_id).get(rule.rule_id).put(word,
							gameRuleWordCount.get(rule.game_id).get(rule.rule_id).get(word));
				} else {
					newGameRuleWordCount.get(rule.game_id).get(rule.rule_id).put(word, 0);
				}
			}
		}
		// 更新指针
		this.gameRuleWordCount = newGameRuleWordCount;
		log.warn("gameRuleWordCount reload done: " + gameRuleWordCount.size());
	}
	
	/**
	 * 判断评论中是否含有该词，n元组拼接匹配
	 * @param record
	 * @param word
	 * @return
	 */
	public boolean contains(Record record, String word) {
		// 取分词
		String[] segWords = record.reviewSeg.split("\t");
		for (int i = 0; i < segWords.length; i++) {
			/** 两个上限满足其一
			 *  1. n个分词组合为元组的个数上限
			 *  2. 分词总个数上限
			 */
			for (int j = i + 1;
				 j < i + 1 + ConfigUtils.getIntValue("ntuple_words_compose") && j <= segWords.length;
				 j++) {
				// 拼接 i -> j 分词，组合在一起匹配
				String mkWord = StringUtils.join(Arrays.copyOfRange(segWords, i, j), "");
//				System.out.println("i: " + i + " j: " + j + " word: " + mkWord);
				
				// 判断该词是否与n元组相同
				if (word.equals(mkWord)) {
					return true;
				}
			}
		}
		return false;
	}
	
	// 测试 contains(record, word) 方法 
	public static void main(String[] args) {
		Record record = new Record();
		record.reviewSeg = "aa	bb	cc	dd	ee	ff	gg	hh";
		System.out.println(new CountLayer().contains(record, "bbcc"));
	}
	
}
