package sparkstreaming_action.count.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

/**
 * 简单版的垃圾过滤
 * @author wayne
 *
 */
public class TrashFilterUtils {
	
	// 这些变量未使用，功能可扩展
	static final int TYPE_FORUM_MAIN = 1;
	static final int TYPE_FORUM_SUB = 2;
	static final int TYPE_STORE = 3;
	static final int TYPE_WEIBO = 4;
	static final int TYPE_WEIBO_COMMENT = 5;
	static final int TYPE_WEIBO_FOWARD = 6;
	
	// 文本长度上限
	static int TOO_LONG_TEXT_LEN = 10000;
	
	// 中文正则
	static Pattern patternChinese = Pattern.compile("[\\u4E00-\\u9FBF]+");
	// 标签正则
	static Pattern patternLabel = Pattern.compile("<[^>]+>");
	// 匹配文件的根路径
	static final String patternFilePath = "/";
	// 匹配文件：应用商店
	static final String appStorePatternFile = "patterns_appstore.txt";
	// 匹配文件：论坛
	static final String forumPatternFile = "patterns_forum.txt";
	
	// App应用商店评论 文本匹配列表
	static ArrayList<Pattern> appStorePatternList = null;
	// 论坛评论 文本匹配列表
	static ArrayList<Pattern> forumPatternList = null;
	
	static Logger log = Logger.getLogger(TrashFilterUtils.class);
	
	// 静态加载块：静态加载正则文本
	static {
		reload();
	}
	
	// 重载
	public static void reload() {
		ArrayList<Pattern> newAppStorePatternList = new ArrayList<Pattern>();
		ArrayList<Pattern> newForumPatternList = new ArrayList<Pattern>();
		// 解析文件的正则文本内容
		parsePatternFile(appStorePatternFile, newAppStorePatternList);
		parsePatternFile(forumPatternFile, newForumPatternList);
		appStorePatternList = newAppStorePatternList;
		forumPatternList = newForumPatternList;
	}
	
	/**
	 * 解析模式匹配文件
	 * @param fileName
	 * @param patternList
	 */
	private static void parsePatternFile(String fileName, ArrayList<Pattern> patternList) {
		// 自定义标签符号替换正则
		Pattern patternSymbol = Pattern.compile("<[^>]+>");
		InputStream is = null;
		try {
			// 读入文件
			is = TrashFilterUtils.class.getResourceAsStream(patternFilePath + fileName);
			BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"), 1024);
			
			// 解析阶段：0|不做任何处理；1|加载自定义标签符号；2|加载匹配正则
			int stage = 0; // 1: load symbol; 2: load patterns
			
			/** 自定义标签符号集合：用于替换正则中的标签符号
			 * 
			 * 如下（stage-1）为自定义标签符号：
			 *   NonTerminal-Rule
			 *   <qq>:qq|扣扣|釦釦
			 *   <微信>:微信|徽信|威信|薇信|威心|葳信|维芯|薇芯|威芯|微芯|wei信|weixin|wechat|vx
			 *   <数字英文>:[0-9a-zA-Z]
			 * 如下（stage-2）为包含自定义标签符号的正则：（需要把标签符号替换，很像 宏替换）
			 *   Terminal-Rule
			 *   "(加|加入)(我|这|这个).*?(<微信>|<qq>|pp群|群|裙|qun|<数字英文>{7,})":[Topic="游戏行为"]
			 *   
			 */
			HashMap<String, String> symbolMap = new HashMap<String, String>();
			
			log.info(fileName + " rules:");
			// 按行读入处理
			for (String line = reader.readLine(); line != null; line = reader.readLine()) {
				
				if ("".equals(line)) {
					continue;
				}
				if ("NonTerminal-Rule".equals(line)) { // 1. change stage
					stage = 1;
				} else if ("Terminal-Rule".equals(line)) {
					stage = 2;
				} else if (stage == 1) { // 2. stage 1: get symbol
					/** 解析阶段 1
					 *  添加自定义标签到集合
					 */
					String[] splits = line.split(":");
					if (splits.length != 2) {
						log.error("symbol line: " + line + ", splits is not 2");
						continue;
					}
					log.info("symbol: " + splits[0] + ", content: " + splits[1]);
					symbolMap.put(splits[0], splits[1]);
				} else if (stage == 2) { // 3. stage 2: pattern
					/** 解析阶段 2
					 *  替换正则中的自定义标签符号
					 */
					// 取最后一个冒号前的正则匹配串，如："aaa:cccc:ddd":bbb -> aaa:cccc:ddd
					String oldPatternStr = line.substring(1, line.lastIndexOf(":") - 1);
					String newPatternStr = oldPatternStr;
					boolean replaceSucc = true;
					// 匹配正则中的自定义标签符号
					Matcher matcher = patternSymbol.matcher(oldPatternStr);
					while (matcher.find()) {
						String symbol = matcher.group();
						// 一条正则中，只要有一个标签符号未定义，就丢弃该正则
						if (!symbolMap.containsKey(symbol)) {
							log.debug("get symbol: " + symbol + " fails for pattern: " + oldPatternStr);
							replaceSucc = false; // 替换失败
							break;
						}
						// 替换正则中的标签符号
						newPatternStr = newPatternStr.replace(symbol, symbolMap.get(symbol));
					}
					// 只有正则中的标签符号替换成功时，才能将这条正则加入正则集合中
					if (replaceSucc) {
						log.info("old pattern: " + oldPatternStr + ", new pattern: " + newPatternStr);
						try {
							Pattern pattern = Pattern.compile(newPatternStr);
							if (pattern != null) {
								// 加入正则集合
								patternList.add(pattern);
							}
						} catch (Exception e) {
							log.error("compile pattern fails: " + newPatternStr);
						}
					}
				}
			}
			log.info("pattern num: " + patternList.size());
		} catch (Exception e) {
			log.error("parse pattern file error, ex: " + ExceptionUtils.getStackTrace(e));
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (Exception e) {
				}
			}
		}
	}
	
	/**
	 * 判断是否包含中文
	 * @param text
	 * @return
	 */
	private static boolean containsChinese(String text) {
		try {
			return patternChinese.matcher(text).find();
		} catch (Exception e) {
			return true;
		}
	}
	
	/**
	 * 去除文本中的标签，保留标签以外的内容
	 * @param text
	 * @return
	 */
	private static String trimLabel(String text) {
		try {
			Matcher matcher = patternLabel.matcher(text);
			if (matcher.find()) {
				// 使用 replaceAll()替换匹配子串（标签）为空串
				return patternLabel.matcher(text).replaceAll("").trim();
			}
		} catch (Exception e) {
		}
		return text;
	}
	
	/**
	 * 判断是否为垃圾信息
	 * @param text
	 * @return
	 */
	public static boolean isTrash(String text) {
		// 1. 如果不包含中文
		if (!containsChinese(text)) {
			return true;
		}
		
		// 2. 如果去除标签后，文本太长
		String trimText = trimLabel(text);
		if (trimText.length() >= TOO_LONG_TEXT_LEN) {
			return true;
		}
		
		// 3. 如果包含模板文件中的匹配信息
		for (Pattern pattern : appStorePatternList) {
			if (pattern.matcher(trimText).find()) {
				return true;
			}
		}
		for (Pattern pattern : forumPatternList) {
			if (pattern.matcher(text).find()) {
				return true;
			}
		}
		
		return false;
	}
	
	// 测试
	public static void main(String[] args) {
		// 私有方法 containsChinese()测试
		System.out.println("呵呵 contains chinese:" + containsChinese("呵呵"));
		System.out.println("呵呵 ，。/ contains chinese:" + containsChinese("呵呵"));
		System.out.println("。 contains chinese:" + containsChinese("，"));
		System.out.println("， contains chinese:" + containsChinese("。"));
		System.out.println("112fsafdasf~!@#$%^&*()_+11123454606;k;, contains chinese:"
				+ containsChinese("112fsafdasf~!@#$%^&*()_+11123454606;k;,"));
		System.out.println(" contains chinese:" + containsChinese(""));

		// 私有方法 filterHtml()测试
		System.out.println(
				" <hehe haha> asfsdf </hehe haha> trim is:" + trimLabel("  <hehe haha> asfsdf </hehe haha>  "));
		System.out.println("不是html trim is:" + trimLabel(" 不是html "));
		System.out.println(" trim is:" + trimLabel(""));
		
		// 测试垃圾信息
		String text = "42398485dasdfah";
		System.out.println("isTrash:" + isTrash(text) + ": " + text);
		text = "<sdf>asfnfe得分</sdf>";
		System.out.println("isTrash:" + isTrash(text) + ": " + text);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < TOO_LONG_TEXT_LEN + 10; i++)
			sb.append("啊");
		System.out.println("isTrash:" + isTrash(sb.toString()) + ": 10010 啊");
		text = "加我QQ00312601";
		System.out.println("isTrash:" + isTrash(text) + ": " + text);
		text = "哈哈没人都给发3元红包";
		System.out.println("isTrash:" + isTrash(text) + ": " + text);
	}
}
