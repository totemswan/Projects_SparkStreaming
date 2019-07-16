package sparkstreaming_action.statistic.service

import scala.collection.mutable.Map
import sparkstreaming_action.statistic.entity.MonitorGame
import org.junit.Test

/** 
 *  SegmentService Test
 */
class SegmentServiceTest {
  
  @Test
  def testDemo: Unit= {
    val reviewJson0 = ""
    val reviewJson1 = """{ "gameId":"2", "review":"我们是共产主义接班人" }"""
    val reviewJson2 = """{ "gameId":"1", "review":"我们是共产主义接班人" }"""
    val reviewJson3 = """{ "gameId":"1", "review":"" }"""
    val reviewJson4 = """{ "gameId":"2301","review":"说实话 作为一个手机游戏 你很成功 成功到弱智 孤儿 自大狂 乱七八糟的人一大堆都玩 小孩子氪金什么问题也一大堆 但为什么我之前没想过来写评论 是我觉得没必要反正上了王者无欲无求 最后问题来了 s5左右上的王者当时是ios 是名v6（深受fgo毒害喜欢抽奖，大部分电卷都用来抽奖了，有武则天)然后就弃坑了 直到前不久 觉得没事做就又下了回来（之前的ios手机被我捞船玩烧了）然后就见识到了你天美是真恶心 我是真的菜 不能一打五 到了钻石连胜五次必碰到*** 然后就要开始连跪 你想认真玩 可以 结果队友菜还骚话不断影响心情 这次新号一分钱没充没英雄 只能寄希望给队友 不然我当时江苏省前50的雅典娜 这多了个星耀在王者中间正常还是带的动的低端局的吧 然而并没有 只好老老实实玩辅助（鱼，项羽） 上单（只有凯（实在筹不到18888买雅典娜)和曹操）养队友（儿子)一路混上了钻石 网却总是460 我就纳闷了 还能是我家网烂到这种程度么 那我用移动卡 电信卡流量你还能这样？行 你说有个加速器是什么至尊通道 我买 但你想让我买皮肤什么的做梦 然后网络就奇迹般的好了 呵呵 那我就玩射手速度点上分 非常顺利的上了钻一 然后心态就彻底炸了 真***的不想玩就别玩 不要刚进来就一副自己很nb的样子 然后被人打崩了怪队友 我一点也不怪队友 我怪的是天美你的***匹配系统 当年就感觉*** 现在更*** 劳资是不是要次次评分最低你才给点正常人给我？是不是只有我一直输 我才配赢？能不能请你把有脑子的没脑子的分两个频道 别跟带小孩一样 评分一高 一连胜 就nm要带小孩 凭什么？玩个游戏 谁不是来找成就感的 你把我对手弄得再强我没意见 至少队友要知道大概思路怎么赢 你把弱智放进来 我只想说***我也打不过" }"""
    val monitorGames = Map[Int, MonitorGame](1 -> new MonitorGame(1, "a"), 2301 -> new MonitorGame(2301, "b"))
    SegmentService.mapSegment(reviewJson0, monitorGames)
    SegmentService.mapSegment(reviewJson1, monitorGames)
    SegmentService.mapSegment(reviewJson2, monitorGames)
    SegmentService.mapSegment(reviewJson3, monitorGames)
    
    val option: Option[(Int, String)] = SegmentService.mapSegment(reviewJson4, monitorGames)
    if (option.isEmpty) {
      println(s"option is empty, reviewJson: ${reviewJson4}")
    } else {
      println(s"Segment success option is not empty, reviewJson: ${reviewJson4}")
    }
  }
}