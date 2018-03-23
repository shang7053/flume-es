本flume插件提供，kafkasource、kafkachannel以及essink，对应版本如下：  
flume1.7.0+  
kafka2.12-0.10.2.1  
elasticsearch6.1.0  
其他版本未试验过。  
flume配置如下：   
agent.sources = s1  
agent.sinks = k1  
agent.channels = c1  
  
#每30分钟自动刷新一次kafka主题列表，从消息体中抓取时间（如无则当前时间）和ip（如无则127.0.0.1）  
agent.sources.s1.type = com.scc.flume.source.kafkasource.KafkaSource  
agent.sources.s1.channels = c1  
agent.sources.s1.bootstrap.servers = 172.16.40.4:9092,172.16.40.5:9092  
agent.sources.s1.topics = log_data_sync_base  
agent.sources.s1.group.id = flume  
agent.sources.s1.topic.prefix=log_  
#agent.sources.s1.topic.suffix=log_  
#agent.sources.s1.topics=aa,bb,cc  
 
#kafkachannel
#agent.channels.c1.type=com.scc.flume.channel.kafkachannel.KafkaChannel          
#agent.channels.c1.bootstrap.servers = 172.16.40.4:9092,172.16.40.5:9092  
#agent.channels.c1.topic=flume_channel_log4j2flume   

#rabbitchannel
agent.channels.c1.type=com.scc.flume.channel.rabbitchannel.RabbitChannel
agent.channels.c1.capacity=100
agent.channels.c1.address=172.16.40.2:5672,172.16.40.3:5672
agent.channels.c1.uname=root
agent.channels.c1.pwd=root
agent.channels.c1.exchange.name=flume_channel_log

  
#自动拿topic当index，type为kafka_flume_log  
agent.sinks.k1.type = com.scc.flume.sink.essink.EsSink  
agent.sinks.k1.channel = c1  
agent.sinks.k1.address=172.16.40.4:9300  
agent.sinks.k1.cluster.name=elasticsearch  
  
注意：   
	删除flume的lib下kafka相关依赖、scala相关依赖  
功能：  
	1、自动发现新topic（30分钟更新一次），可按前缀、后缀或者指定topic,多个规则同时存在时，取并集。  
	2、拿topic的名称当做es的index  
