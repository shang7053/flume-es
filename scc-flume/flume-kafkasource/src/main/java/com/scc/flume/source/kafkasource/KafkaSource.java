package com.scc.flume.source.kafkasource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractPollableSource;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * @ClassName: KafkaSource
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author shangchengcai@voole.com
 * @date 2017年12月18日 上午9:40:46
 * 
 */
public class KafkaSource extends AbstractPollableSource implements Configurable {
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);
	private String servers;
	private String groupid;
	private String auto_commit;
	private String commit_interval;
	private String topic_prefix;
	private String topic_suffix;
	private String topics;
	private Class<Deserializer<String>> key_deserializer;
	private Class<Deserializer<String>> value_deserializer;
	private Consumer<String, String> consumer;
	private KafkaUtil kafkaUtil;
	private ConsumerRecords<String, String> records;

	/*
	 * (非 Javadoc) <p>Title: doProcess</p> <p>Description: </p>
	 * 
	 * @return
	 * 
	 * @throws EventDeliveryException
	 * 
	 * @see org.apache.flume.source.AbstractPollableSource#doProcess()
	 */
	@Override
	protected Status doProcess() throws EventDeliveryException {
		LOGGER.debug("--------------------start kafka Source process--------------------");
		try {
			this.consumer.subscribe(this.getTopicSet());
			this.records = this.consumer.poll(3000);
			if (this.records.isEmpty()) {
				return Status.READY;
			}
			Map<String, String> headers = new HashMap<>();
			List<Event> events = new ArrayList<>();
			for (ConsumerRecord<String, String> record : this.records) {
				String msg = record.value();
				headers.put("@topic", record.topic());
				headers.put("@timestamp", Long.valueOf(StrUtil.getFirstDate(msg).getTime()).toString());
				headers.put("@ip", StrUtil.getFirstIp(msg));
				headers.put("@level", StrUtil.getLevel(msg));
				headers.put("@offset", Long.valueOf(record.offset()).toString());
				events.add(EventBuilder.withBody(msg.getBytes(), headers));
			}
			LOGGER.info("events size={}", events.size());
			this.getChannelProcessor().processEventBatch(events);
		} catch (Exception e) {
			LOGGER.error("process error!{}", e);
			return Status.BACKOFF;
		}
		LOGGER.debug("--------------------end kafka Source process--------------------");
		return Status.READY;
	}

	/*
	 * (非 Javadoc) <p>Title: doConfigure</p> <p>Description: </p>
	 * 
	 * @param context
	 * 
	 * @throws FlumeException
	 * 
	 * @see org.apache.flume.source.BasicSourceSemantics#doConfigure(org.apache.flume.Context)
	 */
	@Override
	protected void doConfigure(Context context) throws FlumeException {
		LOGGER.info("-------------------start config kafka source--------------------");
		this.servers = context.getString(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
		Preconditions.checkNotNull(this.servers,
				"kafka " + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + " can not be null!");
		this.groupid = context.getString(ConsumerConfig.GROUP_ID_CONFIG);
		Preconditions.checkNotNull(this.servers, "kafka " + ConsumerConfig.GROUP_ID_CONFIG + " can not be null!");
		this.auto_commit = context.getString(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		this.commit_interval = context.getString(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		this.topic_prefix = context.getString("topic.prefix");
		this.topic_suffix = context.getString("topic.suffix");
		this.topics = context.getString("topics");
		String key_deserializer_classname = context.getString(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		String value_deserializer_classname = context.getString(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		LOGGER.info(
				"load config,servers={},groupid={},auto_commit={},commit_interval={},topic_prefix={},topic_suffix={},topics={},key_deserializer={},value_deserializer={}",
				new Object[] { this.servers, this.groupid, this.auto_commit, this.commit_interval, this.topic_prefix,
						this.topic_suffix, this.topics, key_deserializer_classname, value_deserializer_classname });
		try {
			this.key_deserializer = (Class<Deserializer<String>>) Class.forName(key_deserializer_classname);
			this.value_deserializer = (Class<Deserializer<String>>) Class.forName(value_deserializer_classname);
		} catch (ClassNotFoundException e) {
			LOGGER.error("init kafka deserializer class faild!{}", e);
			e.printStackTrace();
		}
		LOGGER.info("-------------------end config kafka source--------------------");
	}

	/*
	 * (非 Javadoc) <p>Title: doStart</p> <p>Description: </p>
	 * 
	 * @throws FlumeException
	 * 
	 * @see org.apache.flume.source.BasicSourceSemantics#doStart()
	 */
	@Override
	protected void doStart() throws FlumeException {
		LOGGER.info("-------------------do start kafka source--------------------");
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.servers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupid);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, this.auto_commit);
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, this.commit_interval);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.key_deserializer);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.value_deserializer);
		this.consumer = new KafkaConsumer<>(props);
		this.kafkaUtil = new KafkaUtil(this.consumer, this.topic_prefix, this.topic_suffix);
		LOGGER.info("load pre-topic set,pre={}", this.topic_prefix);
		Set<String> topicsSet = this.getTopicSet();
		Preconditions.checkArgument(topicsSet.size() > 0, "can not match topic from kafka!please check you config!");
		// first poll will cost a lot time,so init it first，and avoid org.apache.kafka.common.errors.InterruptException:
		// java.lang.InterruptedException
		this.consumer.subscribe(topicsSet);
		this.records = this.consumer.poll(3000);
		LOGGER.info("topics={}", topicsSet);
		LOGGER.info("-------------------end do start kafka source--------------------");
	}

	/**
	 * @Title: getTopicSet
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @author shangchengcai@voole.com
	 * @date 2017年12月19日 上午10:33:22
	 * @return
	 */
	private Set<String> getTopicSet() {
		Set<String> topicsSet = new HashSet<>();
		if (StringUtils.isNotBlank(this.topic_prefix)) {
			topicsSet.addAll(this.kafkaUtil.getTopics(TopicNameModel.PREFIX));
		}
		LOGGER.info("load suf-topic set,pre={}", this.topic_suffix);
		if (StringUtils.isNotBlank(this.topic_prefix)) {
			topicsSet.addAll(this.kafkaUtil.getTopics(TopicNameModel.SUFFIX));
		}
		LOGGER.info("load topics set,topics={}", this.topics);
		if (StringUtils.isNotBlank(this.topic_prefix)) {
			topicsSet.addAll(this.splitStr2Set(this.topics));
		}
		return topicsSet;
	}

	/**
	 * @Title: splitStr2Set
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @author shangchengcai@voole.com
	 * @date 2017年12月18日 下午3:09:18
	 * @param topics2
	 * @return
	 */
	private Collection<? extends String> splitStr2Set(String str) {
		Set<String> ret = new HashSet<>();
		if (StringUtils.isNotBlank(str)) {
			String[] temp = str.split(",");
			for (String t : temp) {
				ret.add(t);
			}
		}
		return ret;
	}

	/*
	 * (非 Javadoc) <p>Title: doStop</p> <p>Description: </p>
	 * 
	 * @throws FlumeException
	 * 
	 * @see org.apache.flume.source.BasicSourceSemantics#doStop()
	 */
	@Override
	protected void doStop() throws FlumeException {
		LOGGER.info("-------------------start stop kafka source--------------------");
		this.consumer.close();
		LOGGER.info("-------------------end stop kafka source--------------------");
	}

}
