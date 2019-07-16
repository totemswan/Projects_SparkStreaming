package sparkstreaming_action.count.util;

import java.util.Date;

import org.junit.Test;

public class TimeUtilsTest {

	@Test
	public void test() {
		System.out.println("\n--------------testTimeUtils---------------");
		
		System.out.println("\n--------------test timeToString---------------");
		// 以 s秒 为单位
		System.out.println(TimeUtils.timeToString(new Date().getTime() / 1000));
		
		System.out.println("\n--------------test getStartOfTime---------------");
		// 以 s秒 为单位
		Long startTime = TimeUtils.getStartOfTime(new Date().getTime() / 1000);
		String startTimeStr = new Date(startTime * 1000).toString();
		System.out.println(startTimeStr + " : " + startTime);
		
		System.out.println("\n--------------test getEndOfTime---------------");
		// 以 s秒 为单位
		Long endTime = TimeUtils.getEndOfTime(new Date().getTime() / 1000);
		String endTimeStr = new Date(endTime * 1000).toString();
		System.out.println(endTimeStr + " : " + endTime);
		
		System.out.println("\n--------------test currentTimeSeconds---------------");
		// 以 s秒 为单位
		Long curTime = TimeUtils.currentTimeSeconds();
		String curTimeStr = new Date(curTime * 1000).toString();
		System.out.println(curTimeStr + " : " + curTime);
		
	}
}
