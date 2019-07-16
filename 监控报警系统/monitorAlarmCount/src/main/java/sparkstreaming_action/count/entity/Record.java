package sparkstreaming_action.count.entity;

/**
 * Spark过滤后的Json记录
 * @author wayne
 *
 */
public class Record {

	public String review; // 评论文本
	public String reviewSeg; // 评论文本分词结果
	public int gameId; // 游戏ID
	public String gameName; // 游戏名称
	
	@Override
	public String toString() {
		return String.format("gameId: %d\tgameName: %s\treview: %s\nreviewSeg: %s", 
				gameId, gameName, review, reviewSeg);
	}
}
