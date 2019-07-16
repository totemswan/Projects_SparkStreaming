package sparkstreaming_action.count.util;

import org.junit.Test;

public class ConfigUtilsTest {

	@Test
	public void test() {
		System.out.println("\n--------------testConfigUtils---------------");
		System.out.println("get none: " + ConfigUtils.getConfig("none"));
		System.out.println("get username: " + ConfigUtils.getConfig("username"));
		System.out.println("get boolean value frombegin: " + ConfigUtils.getConfig("frombegin"));
		System.out.println("get boolean value frombegin: " + ConfigUtils.getBooleanValue("frombegin"));
		System.out.println("get int value reload_interval: " + ConfigUtils.getIntValue("reload_interval"));
	}
}
