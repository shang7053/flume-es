package com.scc.flume.channel.kafkachannel;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName: ChannelCallback
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author shangchengcai@voole.com
 * @date 2017年12月20日 下午4:58:01
 * 
 */
public class ChannelCallback implements Callback {
	private static final Logger log = LoggerFactory.getLogger(ChannelCallback.class);
	private int index;
	private long startTime;

	public ChannelCallback(int index, long startTime) {
		this.index = index;
		this.startTime = startTime;
	}

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if (exception != null) {
			log.trace("Error sending message to Kafka due to " + exception.getMessage());
		}
		if (log.isDebugEnabled()) {
			long batchElapsedTime = System.currentTimeMillis() - this.startTime;
			if (metadata != null) {
				log.debug("Acked message_no " + this.index + ": " + metadata.topic() + "-" + metadata.partition() + "-"
						+ metadata.offset() + "-" + batchElapsedTime);
			}
		}
	}

}
