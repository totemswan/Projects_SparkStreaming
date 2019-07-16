package sparkstreaming_action.count.service;

import org.junit.Test;

import sparkstreaming_action.count.entity.Record;

public class FilterLayerTest {

	@Test
	public void test() {
		// 测试过滤项，及垃圾记录的文件输出
		System.out.println("\n-------------test FilterLayer filter--------------");
		Record record = new Record();
		record.review = "<aaa>";
		FilterLayer fl = new FilterLayer();
		System.out.println(fl.filter(record));
		
		// 测试过滤HTML标签
		System.out.println("\n-------------test FilterLayer trimHTMLTag--------------");
		System.out.println(
				FilterLayer.trimHTMLTag(
						"<script>测试数据aaa</script><style>red</style><div><p>通过</div>"
						)
				); // 最终输出："通过"
		System.out.println(
				FilterLayer.trimHTMLTag(
						"<p>你好阿，这游戏真的不错</p><img src='http://11213sdfasf.jpg' />"
						)
				); // 最终输出："你好阿，这游戏真的不错"
	}
}
