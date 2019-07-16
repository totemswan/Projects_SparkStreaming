package sparkstreaming_action.count.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 时间类通用函数
 * @author wayne
 *
 */
public class TimeUtils {

	/**
	 * 将指定时间戳（单位：s）格式化为字符串时间
	 * @param timeSec
	 * @return
	 */
	public static String timeToString(Long timeSec) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(new Date(timeSec * 1000));
	}
	
	/**
	 * 获取指定时间戳（单位：s）在一天中的开始时间 00:00:00
	 * @param timeSec
	 * @return
	 */
	public static Long getStartOfTime(Long timeSec) {
		// 当前时间
		Calendar cur = Calendar.getInstance();
		// 设置为指定时间
		cur.setTimeInMillis(timeSec * 1000);
		// 设置时、分、秒、毫秒都为0
		cur.set(Calendar.HOUR_OF_DAY, 0);
		cur.set(Calendar.MINUTE, 0);
		cur.set(Calendar.SECOND, 0);
		cur.set(Calendar.MILLISECOND, 0);
		// 返回秒值
		return cur.getTimeInMillis() / 1000;
	}
	
	/**
	 * 获取指定时间戳（单位：s）在一天中的结束时间，即第二天的开始时间 00:00:00
	 * @param timeSec
	 * @return
	 */
	public static Long getEndOfTime(Long timeSec) {
		return getStartOfTime(timeSec) + 86400; // 返回秒值 86400 = 24 * 3600
	}
	
	/**
	 * 获取当前时间戳（单位：s）
	 * @return
	 */
	public static long currentTimeSeconds() {
		return System.currentTimeMillis() / 1000;
	}
	
}
