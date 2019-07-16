package sparkstreaming_action.count.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.junit.Test;

public class CommonUtilsTest {

	@Test
	public void test() throws UnsupportedEncodingException {
		System.out.println("\n--------------testCommonUtils---------------");
		// 中文路径乱码
		String text = "%e7%9b%91%e6%8e%a7%e6%8a%a5%e8%ad%a6%e7%b3%bb%e7%bb%9f";
		System.out.println(URLDecoder.decode(text, "UTF-8"));
		System.out.println(URLDecoder.decode("中文", "UTF-8"));
		
		// 第一次加载时，文件属于 更改
		System.out.println(CommonUtils.isFileChange(new String[] {"config.properties","log4j.properties"}));
		// 第二次时，文件属于 未更改
		System.out.println(CommonUtils.isFileChange(new String[] {"config.properties","log4j.properties"}));
		try {
			// 线程阻塞 10s，同时修改指定文件
			Thread.sleep(10000);
			// 测试异常打印
			String str = null;
			str.charAt(1);
		} catch (Exception e) {
			System.out.println(CommonUtils.getStackTrace(e));
		}
		// 10s到达时，文件属于 更改
		System.out.println(CommonUtils.isFileChange(new String[] {"config.properties","log4j.properties"}));
	}
}
