package sparkstreaming_action.count.util;

import java.beans.PropertyVetoException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayHandler;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import sparkstreaming_action.count.entity.Alarm;

/**
 * Mysql工具类，利用comemon-DBUtils提供数据库查询服务
 * @author wayne
 *
 */
public class MysqlUtils {

	private static Logger log = Logger.getLogger(MysqlUtils.class);
	// 连接池
	private static ComboPooledDataSource cpds = new ComboPooledDataSource();
	
	// 静态加载块
	static {
		try {
			cpds.setDriverClass("com.mysql.jdbc.Driver");
			cpds.setJdbcUrl(ConfigUtils.getConfig("url"));
			cpds.setUser(ConfigUtils.getConfig("username"));
			cpds.setPassword(ConfigUtils.getConfig("password"));
			// 连接池最小连接数
			cpds.setMinPoolSize(5);
			// 连接池连接数增长步长
			cpds.setAcquireIncrement(5);
			// 连接池最大连接数
			cpds.setMaxPoolSize(20);
			// 语句数上限
			cpds.setMaxStatements(180);
			// 检查连接配置
			cpds.setPreferredTestQuery("SELECT 1");
			// 空闲连接检测周期
			cpds.setIdleConnectionTestPeriod(10000);
		} catch (PropertyVetoException e) {
			log.error(ExceptionUtils.getStackTrace(e));
		}
	}
	
	/**
	 * 获取数据库连接
	 * @return
	 */
	public static Connection getConnection() {
		Connection conn = null;
		try {
			conn = cpds.getConnection();
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		}
		return conn;
	}
	
	/**
	 * 获取指定db table的更新时间
	 * @param table
	 * @return
	 */
	public static long getUpdateTime(String table) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			Map<String, Object> rs = queryByMapHandler(String.format(
					"select TABLE_NAME, UPDATE_TIME from information_schema.TABLES"
					+ " where information_schema.TABLES.TABLE_NAME = '%s';", table));
			if (rs == null || rs.get("UPDATE_TIME") == null)
				return 0L;
			String updateTime = rs.get("UPDATE_TIME").toString();
			return sdf.parse(updateTime).getTime() / 1000; // 时间单位转为 s 秒
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
			return 0L;
		}
	}
	
	/**
	 * 指定表名和对象，进行插入操作
	 * @param table
	 * @param o
	 */
	public static void insert(String table, Object o) {
		Connection conn = null;
		try {
			conn = getConnection();
			QueryRunner runner = new QueryRunner();
			String sql = String.format("insert into %s (%s) values (%s);", table,
					StringUtils.join(getFieldNames(o), ","), 
					StringUtils.join(getFieldValues(o), ","));
			runner.update(conn, sql);
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		} finally {
			destroy(conn);
		}
	}
	
	/**
	 * 返回一个数组，用于将结果集第一行数据转换为数组
	 * @param sql
	 * @return
	 */
	public static Object[] queryByArrayHandler(String sql) {
		Object[] rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
			QueryRunner runner = new QueryRunner();
			rs = runner.query(conn, sql, new ArrayHandler());
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		} finally {
			destroy(conn);
		}
		return rs;
	}
	
	/**
	 * 返回一个集合，集合中的每一项对应结果集指定行中的数据转换后的数组。
	 * @param sql
	 * @return
	 */
	public static List<Object[]> queryByArrayListHandler(String sql) {
		List<Object[]> rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
			QueryRunner runner = new QueryRunner();
			rs = runner.query(conn, sql, new ArrayListHandler());
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		} finally {
			destroy(conn);
		}
		return rs;
	}
	
	/**
	 * 将sql查询的结果集中的第一行转换为键值对，键为列名
	 * @param sql
	 * @return
	 */
	public static Map<String, Object> queryByMapHandler(String sql) {
		Map<String, Object> rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
			QueryRunner runner = new QueryRunner();
			rs = runner.query(conn, sql, new MapHandler());
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		} finally {
			destroy(conn);
		}
		return rs;
	}
	
	/**
	 * 将sql查询的结果集转换为一个集合，集合中的数据为对应行转换的键值对，键为列名
	 * @param sql
	 * @return
	 */
	public static List<Map<String, Object>> queryByMapListHandler(String sql) {
		List<Map<String, Object>> rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
			QueryRunner runner = new QueryRunner();
			rs = runner.query(conn, sql, new MapListHandler());
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		} finally {
			destroy(conn);
		}
		return rs;
	}
	
	/**
	 * 泛型函数，将结果集中的第一行转换为指定Bean
	 * @param sql
	 * @param beanType
	 * @return
	 */
	public static <T> T queryByBeanHandler(String sql, Class<T> beanType) {
		T rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
			QueryRunner runner = new QueryRunner();
			rs = runner.query(conn, sql, new BeanHandler<T>(beanType));
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		} finally {
			destroy(conn);
		}
		return rs;
 	}
	
	/**
	 * 泛型函数，将结果集中所有行转换为指定Bean的List
	 * @param sql
	 * @param beanType
	 * @return
	 */
	public static <T> List<T> queryByBeanListHandler(String sql, Class<T> beanType) {
		List<T> rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
			QueryRunner runner = new QueryRunner();
			rs = runner.query(conn, sql, new BeanListHandler<T>(beanType));
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		} finally {
			destroy(conn);
		}
		return rs;
	}
	
	/**
	 * 释放连接
	 * @param conn
	 */
	public static void destroy(Connection conn) {
		DbUtils.closeQuietly(conn);
	}
	
	/**
	 * 获取对象的所有属性值，返回一个list
	 * @param o
	 * @return
	 */
	private static List<String> getFieldValues(Object o) {
		// 反射获取对象成员变量
		Field[] fields = o.getClass().getDeclaredFields();
		List<String> fieldValues = new ArrayList<String>();
		for (int i = 0; i< fields.length; i++) {
			String name = fields[i].getName();
			if (Objects.equals(name, "id")) {
				continue; // id 一般为自增，无需再插入
			}
			String value = getFieldValuesByName(name, o);
			String type = fields[i].getType().toString();
			if (Objects.equals(type, "class java.lang.String")) {
				value = "'" + value + "'";
			}
			fieldValues.add(value);
		}
		return fieldValues;
	}
	
	/**
	 * 获取对象的所有属性名，返回一个list
	 * @param o
	 * @return
	 */
	private static List<String> getFieldNames(Object o) {
		Field[] fields = o.getClass().getDeclaredFields();
		List<String> fieldNames = new ArrayList<String>();
		for (int i = 0; i < fields.length; i++) {
			String name = fields[i].getName();
			if (Objects.equals(name, "id")) {
				continue; // id 一般为自增，无需再插入
			}
			fieldNames.add(name);
		}
		return fieldNames;
	}
	
	/**
	 * 获取对象属性的所有值，返回一个对象数组
	 * @param fieldName
	 * @param o
	 * @return
	 */
	private static String getFieldValuesByName(String fieldName, Object o) {
		try {
			// 构建对象属性值的getter方法名
			String firstLetter = fieldName.substring(0, 1).toUpperCase();
			String getter = "get" + firstLetter + fieldName.substring(1);
			// 利用反射获取对象getter方法
			Method method = o.getClass().getMethod(getter, new Class[] {});
			// 调用getter方法
			Object value = method.invoke(o, new Object[] {});
			return value.toString();
		} catch (Exception e) {
			return null;
		}
	}
	
	// 测试
	public static void main(String[] args) {
		// 测试 私有方法 getField*()
		Alarm alarm = new Alarm();
		alarm.game_id = 1;
		alarm.game_name = "王者荣耀";
		System.out.println(StringUtils.join(getFieldNames(alarm), ","));
		System.out.println(StringUtils.join(getFieldValues(alarm), ","));
	}
}
