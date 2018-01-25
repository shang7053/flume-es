package com.scc.flume.sink.essink;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * @ClassName: EsSink
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author shangchengcai@voole.com
 * @date 2017年12月18日 下午4:23:07
 * 
 */
public class EsSink extends AbstractSink implements Configurable {
	private static final Logger LOGGER = LoggerFactory.getLogger(EsSink.class);
	private String address;
	private String cluster_name;
	private TransportClient client;

	/*
	 * (非 Javadoc) <p>Title: process</p> <p>Description: </p>
	 * 
	 * @return
	 * 
	 * @throws EventDeliveryException
	 * 
	 * @see org.apache.flume.Sink#process()
	 */
	@Override
	public Status process() throws EventDeliveryException {
		LOGGER.debug("-------------------start es process--------------------");
		Channel channel = this.getChannel();
		Transaction transaction = channel.getTransaction();
		List<XContentBuilder> datas = new ArrayList<XContentBuilder>();
		try {
			transaction.begin();
			Event event = channel.take();
			if (event == null) {
				LOGGER.debug("no event recive……");
				transaction.commit();
				return Status.READY;
			}
			List<Map<String, Object>> events = this.deserializeValue(event.getBody());
			LOGGER.info("sink get a event from channel,size={}", events.size());
			if (events.size() < 1) {
				LOGGER.debug("no event recive……");
				transaction.commit();
				return Status.READY;
			}
			BulkRequestBuilder bulkRequest = this.client.prepareBulk();
			for (Map<String, Object> e : events) {
				XContentBuilder builder = jsonBuilder().startObject();
				byte[] body = (byte[]) e.get("body");
				Map<String, String> headers = (Map<String, String>) e.get("headers");
				builder.field("@message", new String(body));
				for (Entry<String, String> header : headers.entrySet()) {
					if (header.getKey().equals("@timestamp")) {
						builder.field(header.getKey(), new Date(Long.valueOf(header.getValue())));
					} else {
						builder.field(header.getKey(), header.getValue());
					}
				}
				builder.endObject();
				datas.add(builder);
				bulkRequest.add(this.client.prepareIndex(headers.get("@topic"), "kafka_flume_log").setSource(builder));
			}
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			LOGGER.info("add data to es ,response status={}", bulkResponse.status());
			transaction.commit();
		} catch (Exception e) {
			LOGGER.error("process es sink happended a error!{}", e);
			transaction.rollback();
			return Status.BACKOFF;
		} finally {
			transaction.close();
			for (XContentBuilder builder : datas) {
				builder.close();
			}
		}
		LOGGER.debug("-------------------end es process--------------------");
		return Status.READY;
	}

	private List<Map<String, Object>> deserializeValue(byte[] value) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bin = new ByteArrayInputStream(value);
		ObjectInputStream obin = new ObjectInputStream(bin);
		return (List<Map<String, Object>>) obin.readObject();
	}

	/*
	 * (非 Javadoc) <p>Title: configure</p> <p>Description: </p>
	 * 
	 * @param context
	 * 
	 * @see org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
	 */
	@Override
	public void configure(Context context) {
		LOGGER.info("-------------------start config es sink--------------------");
		this.address = context.getString("address");
		Preconditions.checkNotNull(this.address, "es address can not be null!");
		this.cluster_name = context.getString("cluster.name");
		Preconditions.checkNotNull(this.address, "es address can not be null!");
		LOGGER.info("-------------------start config es sink--------------------");
	}

	/*
	 * (非 Javadoc) <p>Title: start</p> <p>Description: </p>
	 * 
	 * @see org.apache.flume.sink.AbstractSink#start()
	 */
	@Override
	public synchronized void start() {
		LOGGER.info("-------------------do start es sink--------------------");
		try {
			super.start();
			Settings settings = Settings.builder().put("cluster.name", this.cluster_name).build();
			this.client = new PreBuiltTransportClient(settings);
			String[] addresses = this.address.split(",");
			for (String string : addresses) {
				String[] temp = string.split(":");
				this.client.addTransportAddress(
						new TransportAddress(InetAddress.getByName(temp[0]), Integer.valueOf(temp[1])));
			}
		} catch (UnknownHostException e) {
			LOGGER.error("start es sink happended a error!{}", e);
			e.printStackTrace();
		}
		LOGGER.info("-------------------end do start es sink--------------------");
	}

	/*
	 * (非 Javadoc) <p>Title: stop</p> <p>Description: </p>
	 * 
	 * @see org.apache.flume.sink.AbstractSink#stop()
	 */
	@Override
	public synchronized void stop() {
		LOGGER.info("-------------------do start es source--------------------");
		super.stop();
		this.client.close();
		LOGGER.info("-------------------do start es source--------------------");
	}

}
