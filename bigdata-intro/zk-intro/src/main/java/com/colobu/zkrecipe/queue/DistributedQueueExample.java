package com.colobu.zkrecipe.queue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

/**
 * 分布式队列
 */
public class DistributedQueueExample {
	private static final String PATH = "/example/queue";

	public static void main(String[] args) throws Exception {
		TestingServer server = new TestingServer();
		CuratorFramework client = null;
		DistributedQueue<String> queue = null;
		try {
            //ZK客户端
			client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
            //事件监听器
			client.getCuratorListenable().addListener(new CuratorListener() {
				@Override
				public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
					System.out.println("CuratorEvent: " + event.getType().name());
				}
			});
			client.start();

            //消费者
            QueueConsumer<String> consumer = createQueueConsumer();
            //创建一个队列, 会将消费者绑定到队列上, 因此消费者类似监听器, 当队列中有数据时, 消费者就能从队列中取出数据进行消费
			QueueBuilder<String> builder = QueueBuilder.builder(client, consumer, createQueueSerializer(), PATH);
            queue = builder.buildQueue();
			queue.start();

            //往队列里放入数据
			for (int i = 0; i < 10; i++) {
				queue.put(" test-" + i);
				Thread.sleep((long)(3 * Math.random()));
			}
			
			Thread.sleep(20000);
			
		} catch (Exception ex) {

		} finally {
			CloseableUtils.closeQuietly(queue);
			CloseableUtils.closeQuietly(client);
			CloseableUtils.closeQuietly(server);
		}
	}

    //QueueSerializer提供了对队列中的对象的序列化和反序列化
	private static QueueSerializer<String> createQueueSerializer() {
		return new QueueSerializer<String>(){

			@Override
			public byte[] serialize(String item) {
				return item.getBytes();
			}

			@Override
			public String deserialize(byte[] bytes) {
				return new String(bytes);
			}
			
		};
	}

    //QueueSerializer提供了对队列中的对象的序列化和反序列化
	private static QueueConsumer<String> createQueueConsumer() {

		return new QueueConsumer<String>(){

			@Override
			public void stateChanged(CuratorFramework client, ConnectionState newState) {
				System.out.println("connection new state: " + newState.name());
			}

            //处理队列中的数据的代码逻辑
			@Override
			public void consumeMessage(String message) throws Exception {
				System.out.println("consume one message: " + message);				
			}
			
		};
	}
}
