# -*- coding:UTF-8 -*-
import jieba
cut = jieba.cut
from bottle import route, run

# 利用结巴分词的切词函数
def token(sentence):
	seg_list = list(cut(sentence))
	return " ".join(seg_list)

# 路由地址/token/
@route('/token/:sentence')
def index(sentence):
	result = token(sentence)
	# 返回json格式的结果
	return "{\"ret\":0, \"msg\":\"OK\", \"terms\":\"%s\"}" % result
	
if __name__ == "__main__":
	# 以master:8282启动服务
	run(host="master", port=8282)