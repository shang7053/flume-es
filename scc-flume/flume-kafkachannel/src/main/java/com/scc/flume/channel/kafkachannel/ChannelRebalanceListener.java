package com.scc.flume.channel.kafkachannel;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName: ChannelRebalanceListener
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author shangchengcai@voole.com
 * @date 2017年12月20日 下午4:58:51
 * 
 */
public class ChannelRebalanceListener implements ConsumerRebalanceListener {

	private static final Logger log = LoggerFactory.getLogger(ChannelRebalanceListener.class);
	private AtomicBoolean rebalanceFlag;

	public ChannelRebalanceListener(AtomicBoolean rebalanceFlag) {
		this.rebalanceFlag = rebalanceFlag;
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		for (TopicPartition partition : partitions) {
			log.info("topic {} - partition {} revoked.", partition.topic(), Integer.valueOf(partition.partition()));
			this.rebalanceFlag.set(true);
		}
	}

	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		for (TopicPartition partition : partitions) {
			log.info("topic {} - partition {} assigned.", partition.topic(), Integer.valueOf(partition.partition()));
		}
	}
}
