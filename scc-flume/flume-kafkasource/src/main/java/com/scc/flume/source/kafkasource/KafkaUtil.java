package com.scc.flume.source.kafkasource;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * @ClassName: KafkaUtil
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author shangchengcai@voole.com
 * @date 2017年12月18日 下午2:10:24
 * 
 */
final public class KafkaUtil {
	private static Consumer<String, String> consumer;
	private static String topic_prefix;
	private static String topic_suffix;
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtil.class);
	private static final LoadingCache<String, Set<String>> caches = CacheBuilder.newBuilder().maximumSize(100)
			.refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<String, Set<String>>() {
				@Override
				public Set<String> load(String key) throws Exception {
					switch (key) {
					case "ALL":
						return getKafkaTopics();
					case "PREFIX":
						if (StringUtils.isNotBlank(topic_prefix)) {
							return getKafkaPreTopics(topic_prefix);
						} else {
							return new HashSet<>();
						}
					case "SUFFIX":
						if (StringUtils.isNotBlank(topic_suffix)) {
							return getKafkaSufTopics(topic_suffix);
						} else {
							return new HashSet<>();
						}
					default:
						return getKafkaTopics();
					}
				}
			});

	public Set<String> getTopics(TopicNameModel model) {
		try {
			Set<String> ret = caches.get(model.name());
			if (null != ret) {
				return ret;
			}
		} catch (ExecutionException e) {
			LOGGER.error("get topic happened a error!{}", e);
			e.printStackTrace();
		}
		return new HashSet<>();
	}

	/**
	 * <p>
	 * Title:
	 * </p>
	 * <p>
	 * Description:
	 * </p>
	 * 
	 * @author shangchengcai@voole.com
	 * @date 2017年12月18日 下午2:26:05
	 * @param zkAddress2
	 */
	public KafkaUtil(Consumer<String, String> consumer, String topic_prefix, String topic_suffix) {
		KafkaUtil.consumer = consumer;
		KafkaUtil.topic_prefix = topic_prefix;
		KafkaUtil.topic_suffix = topic_suffix;
	}

	/**
	 * @Title: getKafkaSufTopics
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @author shangchengcai@voole.com
	 * @date 2017年12月18日 下午2:32:11
	 * @param topic_suffix2
	 * @return
	 */
	private static Set<String> getKafkaSufTopics(String suffix) {
		Set<String> topics = getKafkaTopics();
		Set<String> newtopics = new HashSet<>();
		for (String topic : topics) {
			if (topic.endsWith(suffix)) {
				newtopics.add(topic);
			}
		}
		return newtopics;
	}

	/**
	 * @Title: getKafkaPreTopics
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @author shangchengcai@voole.com
	 * @date 2017年12月18日 下午2:32:08
	 * @param topic_prefix2
	 * @return
	 */
	private static Set<String> getKafkaPreTopics(String prefix) {
		Set<String> topics = getKafkaTopics();
		Set<String> newtopics = new HashSet<>();
		for (String topic : topics) {
			if (topic.startsWith(prefix)) {
				newtopics.add(topic);
			}
		}
		return newtopics;
	}

	private static Set<String> getKafkaTopics() {
		LOGGER.info("get topics ");
		Set<String> ret = new HashSet<>();
		try {
			Map<String, List<PartitionInfo>> topics = consumer.listTopics();
			if (null != topics) {
				for (String topic : topics.keySet()) {
					LOGGER.info("get a topic ={}", topic);
					ret.add(topic);
				}
			}
		} catch (Exception e) {
			LOGGER.error("get topic happened a error!{}", e);
			e.printStackTrace();
		}
		return ret;
	}

}
