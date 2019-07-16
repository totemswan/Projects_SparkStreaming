# 使用数据库模式spark
use spark;
# 游戏数据库表
DROP TABLE IF EXISTS games;
CREATE TABLE IF NOT EXISTS games (
	game_id int(11) NOT NULL COMMENT '游戏ID',
    game_name varchar(255) NOT NULL COMMENT '游戏名称',
    PRIMARY KEY(game_id)
) COMMENT '游戏数据库表'
ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO games VALUES
(2301, '王者荣耀');
#(2318, '穿越火线-枪战王者'),
#(43639, '我的世界'), 
#(49995, '第五人格'),
#(57312, '魂武者'),
#(58881, '香肠派对'),
#(59520, '明日之后'),
#(70056, '绝地求生：刺激战场'),
#(74838, '贪婪洞窟2'),
#(83188, '方舟：生存进化');

select * from games;

# 监控游戏库表
DROP TABLE IF EXISTS monitor_games;
CREATE TABLE IF NOT EXISTS monitor_games (
	game_id int(11) NOT NULL COMMENT '游戏ID',
    game_name varchar(255) NOT NULL COMMENT '游戏名称',
    PRIMARY KEY (game_id)
) COMMENT '监控游戏库表'
ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO monitor_games VALUES 
(2301, '王者荣耀');
#(49995, '第五人格');

select * from monitor_games;

# 报警规则库表
DROP TABLE IF EXISTS rules;
CREATE TABLE IF NOT EXISTS rules (
	rule_id int(11) NOT NULL AUTO_INCREMENT COMMENT '规则ID',
    rule_name varchar(255) NOT NULL COMMENT '规则名称',
    game_id int(11) NOT NULL COMMENT '游戏ID',
    game_name varchar(255) NOT NULL COMMENT '游戏名称',
    threshold int(11) NOT NULL COMMENT '阈值',
    words varchar(255) NOT NULL COMMENT '关键词',
    type int(11) NOT NULL COMMENT '规则类型：0|按词平均值；1|按词之和',
    state int(11) NOT NULL COMMENT '规则状态：0|有效；1|失效',
    PRIMARY KEY (rule_id)
) COMMENT '报警规则库表'
ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;

INSERT INTO rules VALUES 
('1', '更新问题', '2301', '王者荣耀', '2', '更新出错 无法登陆', '1', '0'), 
('2', '平衡性问题', '2301', '王者荣耀', '10', '垃圾 连跪 坑 平衡性差 抄袭', '1', '0'), 
('3', '网络问题', '2301', '王者荣耀', '7', '网络问题 卡顿 卡 掉线', '1', '0'),
('4', '用于测试的简单问题', '2301', '王者荣耀', '2', '王者', '1', '0'), 
('5', '平衡性问题', '49995', '第五人格', '10', '玩法单一 平衡性', '1', '0'), 
('6', '程序问题', '49995', '第五人格', '5', 'bug 卸载', '0', '0');

select * from rules;

# 输出报警表
DROP TABLE IF EXISTS alarms;
CREATE TABLE IF NOT EXISTS alarms (
	alarm_id int(11) NOT NULL AUTO_INCREMENT COMMENT '警告ID',
    game_id int(11) NOT NULL COMMENT '游戏ID',
    game_name varchar(255) NOT NULL COMMENT '游戏名称',
	words varchar(255) NOT NULL COMMENT '关键词集合',
    words_freq varchar(255) NOT NULL COMMENT '关键词词频集合',
    rule_id int(11) NOT NULL COMMENT '规则ID',
    rule_name varchar(255) NOT NULL COMMENT '规则名称',
    has_sent int(11) NOT NULL COMMENT '是否已经发送，（默认）0|未发送；1|已发送',
    is_problem int(11) NOT NULL COMMENT '是否真是问题（人工确认），（默认）-1|未确认；0|不是；1|是',
    add_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '警告时间',
    PRIMARY KEY(alarm_id)
) COMMENT '输出报警表'
ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

INSERT INTO alarms VALUES 
('1', '2301', '王者荣耀', '垃圾 连跪 坑 平衡性差 抄袭', '垃圾:3 坑:8 抄袭:0 连跪:0 平衡性差:0 ', '2', '平衡性问题', '0', '-1', '2018-12-11 21:40:06');

select * from alarms;

# 查看表结构
desc alarms;
# 查看列注释
select column_name, column_comment from information_schema.columns where table_schema ='spark' and table_name = 'alarms' ;
# 查看表注释
select table_name, table_comment from information_schema.tables where table_schema = 'spark' and table_name ='alarms';
# 查看表定义DDL
show create table alarms;