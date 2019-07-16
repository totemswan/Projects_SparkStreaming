package sparkstreaming_action.count.util;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/** 
 * 一些公用工具函数
 * @author wayne
 *
 */
public class CommonUtils {

	// 文件上次更新时间，以 [文件名, 时间] 键值对格式存放
	private static Map<String, Long> lastFilesUpdateTime = new HashMap<String, Long>();
	
	/**
	 * 传入多个文件名，任何一个文件有变动则返回true (lastFileUpdateTime)
	 * @param files
	 * @return
	 */
	public static boolean isFileChange(String... files) {
		boolean flag = false;
		for (String file: files) {
			// 获取项目绝对路径下资源文件
			URL res = CommonUtils.class.getResource("/" + file);
			if (res == null) {
				continue;
			}
			String filePath = null;
			try {
				// 防止中文路径乱码
				filePath = URLDecoder.decode(res.getFile(), "UTF-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			long updateTime = new File(filePath).lastModified();
			// 首次加入内存也视为文件更改
			if (!lastFilesUpdateTime.containsKey(file)
					|| lastFilesUpdateTime.get(file).longValue() != updateTime) {
				lastFilesUpdateTime.put(file, updateTime);
				flag = true;
			}
		}
		return flag;
	}
	
	public static String getStackTrace(Exception e) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		return sw.toString(); // stack trace as a string
	}
}
