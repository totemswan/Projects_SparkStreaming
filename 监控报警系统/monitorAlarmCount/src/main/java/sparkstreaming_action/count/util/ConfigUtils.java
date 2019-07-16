package sparkstreaming_action.count.util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * 配置文件信息工具类
 * @author wayne
 *
 */
public class ConfigUtils {

	// 配置文件绝对路径
	final static String resourceFullName = "/config.properties";
	final static Properties props = new Properties();
	
	// 静态加载块
	static {
		InputStreamReader is = null;
		try {
			// 加载配置文件
			is = new InputStreamReader(ConfigUtils.class.getResourceAsStream(resourceFullName), "UTF-8");
			props.load(is);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * get config String value by key
	 * @param key
	 * @return
	 */
	public static String getConfig(String key) {
		return props.getProperty(key);
	}
	
	/**
	 * get config Boolean value by key
	 * @param key
	 * @return
	 */
	public static Boolean getBooleanValue(String key) {
		if ("true".equals(getConfig(key))) {
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * get config Integer value by key
	 * @param key
	 * @return
	 */
	public static Integer getIntValue(String key) {
		return Integer.parseInt(getConfig(key));
	}
}
