package sparkstreaming_action.count.entity;

/**
 * 输出报警表实体类
 * @author wayne
 *
 */
public class Alarm {
	public int game_id; // 游戏ID
	public String game_name; // 游戏名称
	public String words; // 关键词集合
	public String words_freq; // 关键词词频集合
	public int rule_id; // 规则ID
	public String rule_name; // 规则名称
	public int has_sent; // 是否已经发送，（默认）0|未发送；1|已发送
	public int is_problem; // 是否真是问题（人工确认），（默认）-1|未确认；0|不是；1|是
	
	public int getGame_id() {
		return game_id;
	}
	public void setGame_id(int game_id) {
		this.game_id = game_id;
	}
	public String getGame_name() {
		return game_name;
	}
	public void setGame_name(String game_name) {
		this.game_name = game_name;
	}
	public String getWords() {
		return words;
	}
	public void setWords(String words) {
		this.words = words;
	}
	public String getWords_freq() {
		return words_freq;
	}
	public void setWords_freq(String words_freq) {
		this.words_freq = words_freq;
	}
	public int getRule_id() {
		return rule_id;
	}
	public void setRule_id(int rule_id) {
		this.rule_id = rule_id;
	}
	public String getRule_name() {
		return rule_name;
	}
	public void setRule_name(String rule_name) {
		this.rule_name = rule_name;
	}
	public int getHas_sent() {
		return has_sent;
	}
	public void setHas_sent(int has_sent) {
		this.has_sent = has_sent;
	}
	public int getIs_problem() {
		return is_problem;
	}
	public void setIs_problem(int is_problem) {
		this.is_problem = is_problem;
	}
	
	@Override
	public String toString() {
		return String.format("[Alarm] 游戏%s报警：%s (rule_id: %d)", game_name, rule_name, rule_id);
	}
	
}
