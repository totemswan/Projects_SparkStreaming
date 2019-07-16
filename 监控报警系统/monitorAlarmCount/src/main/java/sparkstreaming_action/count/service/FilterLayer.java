package sparkstreaming_action.count.service;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.log4j.Logger;

import sparkstreaming_action.count.entity.Record;
import sparkstreaming_action.count.util.TrashFilterUtils;

/**
 * 过滤层
 *   1. 垃圾过滤
 *   2. 重复过滤（未实现） 
 * @author wayne
 *
 */
public class FilterLayer {

	private static Logger log = Logger.getLogger(FilterLayer.class);
	
	// 过滤记录的文件输出根目录
	private static String filterPath = "filter/";
	
	public FilterLayer() {
	}
	
	/**
	 * 过滤项
	 * @param record
	 * @return
	 */
	public boolean filter(Record record) {
		// 过滤掉垃圾记录
		if (isTrash(record)) {
			// 不管垃圾记录是否成功写道文件中，都返回 true
			log.warn("[filterTrash] record: " + record);
			try {
				FileUtils.writeStringToFile(
						new File(filterPath + "trashFilter" + DateFormatUtils.format(new Date(), "yyyy-MM-dd")), 
						record.toString());
			} catch (IOException e) {
				log.error("[filterTrashWrite] error!", e);
			}
			return true;
		}
		return false;
	}
	
	/**
	 * 判断是否是垃圾项
	 * @param record
	 * @return
	 */
	public boolean isTrash(Record record) {
		return TrashFilterUtils.isTrash(record.review);
	}
	
	/**
	 * 过滤掉HTML标签
	 * @param htmlStr
	 * @return
	 */
	public static String trimHTMLTag(String htmlStr) {
		/**
		 *  \\s 任意空白字符
		 *  \\S 任意非空白字符
		 *  * 匹配0次到多次
		 *  ? 其前面的表达式匹配0次到1次，非贪心的（匹配串尽可能短）
		 */
		// 去除 script style 标签，及标签内的文本内容
		String regEx_script = "<script[^>]*?>[\\s\\S]*?<\\/script>"; // 定义script的正则表达式
		String regEx_style = "<style[^>]*?>[\\s\\S]*?<\\/style>"; // 定义style的正则表达式
		// 去除 标签，保留标签以外的内容
		String regEx_html = "<[^>]+>"; // 定义HTML标签的正则表达式
		
		Pattern p_script = Pattern.compile(regEx_script, Pattern.CASE_INSENSITIVE); // 设置大小写不敏感
		Matcher m_script = p_script.matcher(htmlStr);
		htmlStr = m_script.replaceAll(""); // 过滤script标签及标签中文本，替换匹配串为空白
		
		Pattern p_style = Pattern.compile(regEx_style, Pattern.CASE_INSENSITIVE); // 设置大小写不敏感
		Matcher m_style = p_style.matcher(htmlStr);
		htmlStr = m_style.replaceAll(""); // 过滤style标签及标签中文本，替换匹配串为空白
		
		Pattern p_html = Pattern.compile(regEx_html, Pattern.CASE_INSENSITIVE); // 设置大小写不敏感
		Matcher m_html = p_html.matcher(htmlStr);
		htmlStr = m_html.replaceAll(""); // 过滤script标签符号，替换匹配串为空白
		
		return htmlStr.trim();
	}
	
}
