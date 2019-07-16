package sparkstreaming_action.count.dao;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.map.HashedMap;

import sparkstreaming_action.count.entity.MonitorGame;
import sparkstreaming_action.count.util.MysqlUtils;

public class MonitorGamesDao {

	/**
	 * 获取所有监控游戏
	 * @return
	 */
	public static List<MonitorGame> getMonitorGames() {
		return MysqlUtils.queryByBeanListHandler("select * from monitor_games", MonitorGame.class);
	}
	
	/**
	 * 以 [game_id -> monitorGame] 的形式获取所有监控游戏
	 * @return
	 */
	public static Map<Integer, MonitorGame> getMapMonitorGames() {
		Map<Integer, MonitorGame> mem = new HashedMap<Integer, MonitorGame>();
		
		for (MonitorGame monitorGame : getMonitorGames()) {
			mem.put(monitorGame.game_id, monitorGame);
		}
		return mem;
	}
}
