package sparkstreaming_action.count.util;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import sparkstreaming_action.count.entity.Alarm;
import sparkstreaming_action.count.entity.MonitorGame;

public class MysqlUtilsTest {

	@Test
	public void test() {
		System.out.println("\n--------------testMysqlUtils---------------");
		
		System.out.println("\n---------test getUpdateTime-----------");
		System.out.println("table aaaaaa updateTime: " + MysqlUtils.getUpdateTime("aaaaaa")); // 测试表不存在的情况
		System.out.println("table alarms updateTime: " + MysqlUtils.getUpdateTime("alarms")); // 测试表存在的情况
		System.out.println("table EVENTS updateTime: " + MysqlUtils.getUpdateTime("EVENTS")); // 测试表存在的情况
		
		System.out.println("\n---------test insert-----------");
		Alarm alarm = new Alarm();
		alarm.game_id = 1;
		alarm.game_name = "测试";
		MysqlUtils.insert("alarms", alarm);
		System.out.println("---------test insert over-----------");
		
		System.out.println("\n---------test queryByArrayHandler-----------");
		// 查询结果返回第一行数据
		Object[] objs = MysqlUtils.queryByArrayHandler("select * from monitor_games");
		System.out.println(StringUtils.join(objs, ","));
		
		System.out.println("\n---------test queryByArrayListHandler-----------");
		// 查询结果返回所有行数据
		List<Object[]> listObjs = MysqlUtils.queryByArrayListHandler("select * from monitor_games");
		for (Object[] e : listObjs) {
			System.out.println(StringUtils.join(e, ","));
		}
		
		
		System.out.println("\n---------test queryByMapHandler-----------");
		// 查询结果返回第一行的列名及数据（键值对形式）
		Map<String, Object> mapKV = MysqlUtils.queryByMapHandler("select * from monitor_games");
		System.out.println(StringUtils.join(mapKV, ","));
		
		System.out.println("\n---------test queryByMapListHandler-----------");
		// 查询结果返回所有行的列名及数据（键值对集合形式）
		List<Map<String, Object>> listMapKV = MysqlUtils.queryByMapListHandler("select * from monitor_games");
		for (Map<String, Object> e : listMapKV) {
			System.out.println(StringUtils.join(e, ","));
		}
		
		System.out.println("\n---------test queryByBeanHandler-----------");
		// 查询结果返回第一行的数据对象
		MonitorGame mg = MysqlUtils.queryByBeanHandler("select * from monitor_games", MonitorGame.class);
		System.out.println(mg);
		
		System.out.println("\n---------test queryByBeanListHandler-----------");
		// 查询结果返回所有行的数据对象
		List<MonitorGame> listMg = MysqlUtils.queryByBeanListHandler("select * from monitor_games", MonitorGame.class);
		for (MonitorGame e : listMg) {
			System.out.println(e);
		}
	}
	
}
