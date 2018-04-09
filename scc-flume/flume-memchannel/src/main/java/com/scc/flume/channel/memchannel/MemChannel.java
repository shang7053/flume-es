package com.scc.flume.channel.memchannel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName: memeryChannel2
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author shangchengcai@voole.com
 * @date 2018年1月19日 下午2:27:43
 * 
 */
public class MemChannel extends BasicChannelSemantics {
	private static final Logger LOGGER = LoggerFactory.getLogger(MemChannel.class);
	private static final ConcurrentLinkedQueue FIRST_QUEUE = new ConcurrentLinkedQueue();
	private static final ConcurrentLinkedQueue LAST_QUEUE = new ConcurrentLinkedQueue();
	private Integer capacity;
	private Integer maxtmpsize;
	private Long delaytime;

	/*
	 * (非 Javadoc) <p>Title: createTransaction</p> <p>Description: </p>
	 * 
	 * @return
	 * 
	 * @see org.apache.flume.channel.BasicChannelSemantics#createTransaction()
	 */
	@Override
	protected BasicTransactionSemantics createTransaction() {
		return new MemeryTransaction();
	}

	/*
	 * (非 Javadoc) <p>Title: configure</p> <p>Description: </p>
	 * 
	 * @param context
	 * 
	 * @see org.apache.flume.channel.AbstractChannel#configure(org.apache.flume.Context)
	 */
	@Override
	public void configure(Context context) {
		LOGGER.info("=========================start config memery channel==============================");
		try {
			this.capacity = context.getInteger("capacity", 1);
			this.maxtmpsize = context.getInteger("maxtmpsize", 50000);
			this.delaytime = context.getLong("delaytime", 1000L);
			LOGGER.info("get config capacity={}", this.capacity);
			LOGGER.info("get config maxtmpsize={}", this.maxtmpsize);
			LOGGER.info("get config delaytime={}", this.delaytime);
			new Thread(() -> {
				try {
					while (true) {
						List<Map<String, Object>> datas = new ArrayList<>();
						for (int i = 0; i < MemChannel.this.capacity; i++) {
							byte[] body = (byte[]) FIRST_QUEUE.poll();
							if (null == body) {
								LOGGER.debug("no new message,will be break! current enevt size is {}", datas.size());
								break;
							}
							datas.add(MemChannel.this.deserializeValue(body));
						}
						if (datas.size() > 0) {
							LAST_QUEUE.add(MemChannel.this.serializeValue(datas));
						}
						if (LAST_QUEUE.size() > MemChannel.this.maxtmpsize) {
							LOGGER.info("LAST_QUEUE too many message!sleep for {}ms", MemChannel.this.delaytime);
							Thread.sleep(MemChannel.this.delaytime);
						}
						LOGGER.debug("FIRST_QUEUE size={}", FIRST_QUEUE.size());
					}
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);
					e.printStackTrace();
				}
			}).start();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			e.printStackTrace();
			System.exit(1);
		}
		LOGGER.info("=========================end config memery channel==============================");
	}

	/*
	 * (非 Javadoc) <p>Title: stop</p> <p>Description: </p>
	 * 
	 * @see org.apache.flume.channel.AbstractChannel#stop()
	 */
	@Override
	public synchronized void stop() {
		super.stop();
	}

	private class MemeryTransaction extends BasicTransactionSemantics {
		/*
		 * (非 Javadoc) <p>Title: doPut</p> <p>Description: </p>
		 * 
		 * @param event
		 * 
		 * @throws InterruptedException
		 * 
		 * @see org.apache.flume.channel.BasicTransactionSemantics#doPut(org.apache.flume.Event)
		 */
		@Override
		protected void doPut(Event event) throws InterruptedException {
			LOGGER.debug("-------------------------put channel data to memery start-------------------------");
			try {
				byte[] data = MemChannel.this.serializeValue(event);
				FIRST_QUEUE.add(data);
				LOGGER.info("put event to channel success!data legth={}B", data.length);
				if (FIRST_QUEUE.size() > MemChannel.this.maxtmpsize) {
					LOGGER.info("FIRST_QUEUE too many message!sleep for {}ms", MemChannel.this.delaytime);
					Thread.sleep(MemChannel.this.delaytime);
				}
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
				e.printStackTrace();
			}
			LOGGER.debug("-------------------------put channel data to memery end-------------------------");
		}

		/*
		 * (非 Javadoc) <p>Title: doTake</p> <p>Description: </p>
		 * 
		 * @return
		 * 
		 * @throws InterruptedException
		 * 
		 * @see org.apache.flume.channel.BasicTransactionSemantics#doTake()
		 */
		@Override
		protected Event doTake() throws InterruptedException {
			LOGGER.debug("-------------------------take channel data start-------------------------");
			try {
				byte[] body = (byte[]) LAST_QUEUE.poll();
				if (null != body) {
					LOGGER.info("LAST_QUEUE size={}", LAST_QUEUE.size());
					return EventBuilder.withBody(body, null);
				}
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
				e.printStackTrace();
			}
			LOGGER.debug("-------------------------take channel data end-------------------------");
			return null;
		}

		/*
		 * (非 Javadoc) <p>Title: doCommit</p> <p>Description: </p>
		 * 
		 * @throws InterruptedException
		 * 
		 * @see org.apache.flume.channel.BasicTransactionSemantics#doCommit()
		 */
		@Override
		protected void doCommit() throws InterruptedException {
			// 确认消息
		}

		/*
		 * (非 Javadoc) <p>Title: doRollback</p> <p>Description: </p>
		 * 
		 * @throws InterruptedException
		 * 
		 * @see org.apache.flume.channel.BasicTransactionSemantics#doRollback()
		 */
		@Override
		protected void doRollback() throws InterruptedException {
			// 无需事物
		}
	}

	private byte[] serializeValue(List<Map<String, Object>> datas) throws IOException {
		ByteArrayOutputStream obj = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(obj);
		out.writeObject(datas);
		return obj.toByteArray();
	}

	private byte[] serializeValue(Event event) throws IOException {
		Map<String, Object> data = new HashMap<>();
		data.put("headers", event.getHeaders());
		data.put("body", event.getBody());
		ByteArrayOutputStream obj = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(obj);
		out.writeObject(data);
		return obj.toByteArray();
	}

	private Map<String, Object> deserializeValue(byte[] value) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bin = new ByteArrayInputStream(value);
		ObjectInputStream obin = new ObjectInputStream(bin);
		Map<String, Object> event = (Map<String, Object>) obin.readObject();
		return event;
	}
}
