package sparkstreaming_action.count.entity;

/** 
 * 监控游戏库表实体类
 * @author wayne
 *
 */
public class MonitorGame {

	public int game_id; // 游戏ID
	public String game_name; // 游戏名称
	
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
	
	public String toString() {
		return String.format("game_id: %d\tgame_name: %s", game_id, game_name);
	}
}
