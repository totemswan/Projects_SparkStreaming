# 使用数据库模式 spark
USE spark;
# 用户词库
DROP TABLE IF EXISTS user_words;
CREATE TABLE IF NOT EXISTS user_words (
	id bigint NOT NULL AUTO_INCREMENT,
    word varchar(50) NOT NULL comment '统计关键词',
    add_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '添加时间',
    PRIMARY KEY (id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

# 插入百家姓
insert into user_words (word) values 
('赵'),('钱'),('孙'),('李'),('周'),('吴'),('郑'),('王'),
('冯'),('陈'),('褚'),('卫'),('蒋'),('沈'),('韩'),('杨'),
('朱'),('秦'),('尤'),('许'),('何'),('吕'),('施'),('张'),
('孔'),('曹'),('严'),('华'),('金'),('魏'),('陶'),('姜'),
('戚'),('谢'),('邹'),('喻'),('柏'),('水'),('窦'),('章'),
('云'),('苏'),('潘'),('葛'),('奚'),('范'),('彭'),('郎');

select * from user_words;