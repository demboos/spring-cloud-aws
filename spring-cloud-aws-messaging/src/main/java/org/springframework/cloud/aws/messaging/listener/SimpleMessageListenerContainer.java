/*
 * Copyright 2013-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.aws.messaging.listener;

import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.messaging.MessagingException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.springframework.cloud.aws.messaging.core.QueueMessageUtils.createMessage;

/**
 * @author Agim Emruli
 * @author Alain Sahli
 * @since 1.0
 */
public class SimpleMessageListenerContainer extends AbstractMessageListenerContainer {

	private static final int DEFAULT_WORKER_THREADS = 2;
	private static final String DEFAULT_THREAD_NAME_PREFIX =
			ClassUtils.getShortName(SimpleMessageListenerContainer.class) + "-";

	private boolean defaultTaskExecutor;
	private long backOffTime = 10000;
	private long queueStopTimeout = 10000;
	private int processingThreadsPerQueue = -1;

	private AsyncTaskExecutor taskExecutor;
	private ConcurrentHashMap<String, Future<?>> scheduledListenerFutureByQueue;
	private ConcurrentHashMap<String, List<Future<?>>> scheduledWorkerFuturesByQueue;
	private ConcurrentHashMap<String, Boolean> runningStateByQueue;

	/**
	 * The number of threads processing messages per SQS queue. This value can be
	 * overwritten by per-queue setting using {@link SqsListener} poolSize property.
	 *
	 * @param processingThreadsPerQueue
	 */
	public void setProcessingThreadsPerQueue(int processingThreadsPerQueue) {
		this.processingThreadsPerQueue = processingThreadsPerQueue;
	}

	protected AsyncTaskExecutor getTaskExecutor() {
		return this.taskExecutor;
	}

	public void setTaskExecutor(AsyncTaskExecutor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	/**
	 * @return The number of milliseconds the polling thread must wait before trying to recover when an error occurs
	 * (e.g. connection timeout)
	 */
	public long getBackOffTime() {
		return this.backOffTime;
	}

	/**
	 * The number of milliseconds the polling thread must wait before trying to recover when an error occurs
	 * (e.g. connection timeout). Default is 10000 milliseconds.
	 *
	 * @param backOffTime
	 * 		in milliseconds
	 */
	public void setBackOffTime(long backOffTime) {
		this.backOffTime = backOffTime;
	}

	/**
	 * @return The number of milliseconds the {@link SimpleMessageListenerContainer#stop(String)} method waits for a queue
	 * to stop before interrupting the current thread. Default value is 10000 milliseconds (10 seconds).
	 */
	public long getQueueStopTimeout() {
		return this.queueStopTimeout;
	}

	/**
	 * The number of milliseconds the {@link SimpleMessageListenerContainer#stop(String)} method waits for a queue
	 * to stop before interrupting the current thread. Default value is 10000 milliseconds (10 seconds).
	 *
	 * @param queueStopTimeout
	 * 		in milliseconds
	 */
	public void setQueueStopTimeout(long queueStopTimeout) {
		this.queueStopTimeout = queueStopTimeout;
	}

	@Override
	protected void initialize() {
		if (this.taskExecutor == null) {
			this.defaultTaskExecutor = true;
			this.taskExecutor = createDefaultTaskExecutor();
		}
		super.initialize();
		initializeRunningStateByQueue();
		this.scheduledListenerFutureByQueue = new ConcurrentHashMap<>(getRegisteredQueues().size());
		this.scheduledWorkerFuturesByQueue = new ConcurrentHashMap<>(getRegisteredQueues().size());
	}

	private void initializeRunningStateByQueue() {
		this.runningStateByQueue = new ConcurrentHashMap<>(getRegisteredQueues().size());
		for (String queueName : getRegisteredQueues().keySet()) {
			this.runningStateByQueue.put(queueName, false);
		}
	}

	@Override
	protected void doStart() {
		synchronized (this.getLifecycleMonitor()) {
			scheduleMessageListeners();
		}
	}

	@Override
	protected void doStop() {
		for (Map.Entry<String, Boolean> runningStateByQueue : this.runningStateByQueue.entrySet()) {
			if (runningStateByQueue.getValue()) {
				stop(runningStateByQueue.getKey());
			}
		}
	}

	@Override
	protected void doDestroy() {
		if (this.defaultTaskExecutor) {
			((ThreadPoolTaskExecutor) this.taskExecutor).destroy();
		}
	}

	/**
	 * Create a default TaskExecutor. Called if no explicit TaskExecutor has been specified.
	 * <p>The default implementation builds a {@link org.springframework.core.task.SimpleAsyncTaskExecutor}
	 * with the specified bean name (or the class name, if no bean name specified) as thread name prefix.
	 *
	 * @return a {@link org.springframework.core.task.SimpleAsyncTaskExecutor} configured with the thread name prefix
	 * @see org.springframework.core.task.SimpleAsyncTaskExecutor#SimpleAsyncTaskExecutor(String)
	 */
	protected AsyncTaskExecutor createDefaultTaskExecutor() {
		String beanName = getBeanName();
		ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
		threadPoolTaskExecutor.setThreadNamePrefix(beanName != null ? beanName + "-" : DEFAULT_THREAD_NAME_PREFIX);
		int spinningThreads = 0;

		for (QueueAttributes queueAttributes : this.getRegisteredQueues().values()) {
			spinningThreads += getResolvedPoolSize(queueAttributes.getPoolSize()) + 1; // extra 1 thread for the message listener + n threads for message consumers
		}

		if (spinningThreads > 0) {
			threadPoolTaskExecutor.setCorePoolSize(spinningThreads);
			threadPoolTaskExecutor.setMaxPoolSize(spinningThreads);
		}

		// No use of a thread pool executor queue to avoid retaining message to long in memory
		threadPoolTaskExecutor.setQueueCapacity(0);
		threadPoolTaskExecutor.afterPropertiesSet();

		return threadPoolTaskExecutor;
	}

	private int getResolvedPoolSize(int configuredPoolSize) {
		if (configuredPoolSize < 1) {
			if (processingThreadsPerQueue < 1) {
				return getMaxNumberOfMessages() != null ? getMaxNumberOfMessages() : DEFAULT_WORKER_THREADS;
			} else {
				return processingThreadsPerQueue;
			}
		}

		return configuredPoolSize;
	}

	private void scheduleMessageListeners() {
		for (Map.Entry<String, QueueAttributes> registeredQueue : getRegisteredQueues().entrySet()) {
			startQueue(registeredQueue.getKey(), registeredQueue.getValue());
		}
	}

	protected void executeMessage(org.springframework.messaging.Message<String> stringMessage) {
		getMessageHandler().handleMessage(stringMessage);
	}

	protected void messageExecuted() {}

	/**
	 * Stops and waits until the specified queue has stopped. If the wait timeout specified by {@link SimpleMessageListenerContainer#getQueueStopTimeout()}
	 * is reached, the current thread is interrupted.
	 *
	 * @param logicalQueueName
	 * 		the name as defined on the listener method
	 */
	public void stop(String logicalQueueName) {
		stopQueue(logicalQueueName);
		if (isRunning(logicalQueueName)) {
			Future<?> future = this.scheduledListenerFutureByQueue.remove(logicalQueueName);
			try {
				future.get(this.queueStopTimeout, TimeUnit.MILLISECONDS);
			}  catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (ExecutionException | TimeoutException e) {
				getLogger().warn("Error stopping queue with name: '" + logicalQueueName + "'", e);
			}

			List<Future<?>> queueFutures = this.scheduledWorkerFuturesByQueue.get(logicalQueueName);
			if (queueFutures != null) {
				for (Future<?> workerFuture : queueFutures) {
					if (isRunning(workerFuture)) {
						workerFuture.cancel(true);
					}
				}
				queueFutures.clear();
			}
		}
	}

	protected void stopQueue(String logicalQueueName) {
		Assert.isTrue(this.runningStateByQueue.containsKey(logicalQueueName), "Queue with name '" + logicalQueueName + "' does not exist");
		this.runningStateByQueue.put(logicalQueueName, false);
	}

	public void start(String logicalQueueName) {
		Assert.isTrue(this.runningStateByQueue.containsKey(logicalQueueName), "Queue with name '" + logicalQueueName + "' does not exist");

		QueueAttributes queueAttributes = this.getRegisteredQueues().get(logicalQueueName);
		startQueue(logicalQueueName, queueAttributes);
	}

	/**
	 * Checks if the spinning thread for the specified queue {@code logicalQueueName} is still running (polling for new
	 * messages) or not.
	 *
	 * @param logicalQueueName
	 * 		the name as defined on the listener method
	 * @return {@code true} if the queue is still processing {@code false}.
	 */
	public boolean isRunning(String logicalQueueName) {
		Future<?> future = this.scheduledListenerFutureByQueue.get(logicalQueueName);

		return isRunning(future);
	}

	private boolean isRunning(Future<?> future) {
		return future != null && !future.isCancelled() && !future.isDone();
	}

	protected void startQueue(String queueName, QueueAttributes queueAttributes) {
		if (this.runningStateByQueue.containsKey(queueName) && this.runningStateByQueue.get(queueName)) {
			return;
		}

		this.runningStateByQueue.put(queueName, true);

		BlockingQueue<SignalExecutingRunnable> runnablesQueue = new ArrayBlockingQueue<>(queueAttributes.getReceiveMessageRequest().getMaxNumberOfMessages());

		Future<?> listenerFuture = getTaskExecutor().submit(new AsynchronousMessageListener(queueName, queueAttributes, runnablesQueue));
		this.scheduledListenerFutureByQueue.put(queueName, listenerFuture);

		for (int i = 0; i < getResolvedPoolSize(queueAttributes.getPoolSize()); i++) {
			Future<?> consumerFuture = getTaskExecutor().submit(new AsynchronousMessageConsumer(runnablesQueue));
			addScheduledWorkerFutureByQueue(queueName, consumerFuture);
		}
	}

	private void addScheduledWorkerFutureByQueue(String queueName, Future<?> future) {
		List<Future<?>> queueFutures = this.scheduledWorkerFuturesByQueue.get(queueName);
		if (queueFutures == null) {
			queueFutures = new ArrayList<>();
			this.scheduledWorkerFuturesByQueue.put(queueName, queueFutures);
		}

		queueFutures.add(future);
	}

	private class AsynchronousMessageConsumer implements Runnable {
		private final BlockingQueue<SignalExecutingRunnable> runnablesQueue;

		private AsynchronousMessageConsumer(BlockingQueue<SignalExecutingRunnable> runnablesQueue) {
			this.runnablesQueue = runnablesQueue;
		}

		@Override
		public void run() {
			while (true) {
				try {
					this.runnablesQueue.take().run();
				} catch (InterruptedException e) {
					return;
				}
			}
		}
	}

	private class AsynchronousMessageListener implements Runnable {

		private final QueueAttributes queueAttributes;
		private final String logicalQueueName;
		private final BlockingQueue<SignalExecutingRunnable> runnablesQueue;

		private AsynchronousMessageListener(String logicalQueueName, QueueAttributes queueAttributes, BlockingQueue<SignalExecutingRunnable> runnablesQueue) {
			this.logicalQueueName = logicalQueueName;
			this.queueAttributes = queueAttributes;
			this.runnablesQueue = runnablesQueue;
		}

		@Override
		public void run() {
			while (isQueueRunning() && !Thread.interrupted()) {
				try {
					ReceiveMessageResult receiveMessageResult = getAmazonSqs().receiveMessage(this.queueAttributes.getReceiveMessageRequest());
					CountDownLatch messageBatchLatch = new CountDownLatch(receiveMessageResult.getMessages().size());
					for (Message message : receiveMessageResult.getMessages()) {
						if (isQueueRunning()) {
							MessageExecutor messageExecutor = new MessageExecutor(this.logicalQueueName, message, this.queueAttributes);
							SignalExecutingRunnable signalExecutingRunnable = new SignalExecutingRunnable(messageBatchLatch, messageExecutor);

							this.runnablesQueue.put(signalExecutingRunnable);
						} else {
							messageBatchLatch.countDown();
						}
					}

					messageBatchLatch.await();
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
				} catch (Exception e) {
					getLogger().warn("An Exception occurred while polling queue '{}'. The failing operation will be " +
							"retried in {} milliseconds", this.logicalQueueName, getBackOffTime(), e);
					try {
						//noinspection BusyWait
						Thread.sleep(getBackOffTime());
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
					}
				}
			}
		}

		private boolean isQueueRunning() {
			Boolean value = SimpleMessageListenerContainer.this.runningStateByQueue.get(this.logicalQueueName);

			if (value != null) {
				return value;
			} else {
				getLogger().warn("Stopped queue '" + this.logicalQueueName + "' because it was not listed as running queue.");
				return false;
			}
		}
	}

	private class MessageExecutor implements Runnable {

		private final Message message;
		private final String logicalQueueName;
		private final String queueUrl;
		private final boolean hasRedrivePolicy;
		private final SqsMessageDeletionPolicy deletionPolicy;

		private MessageExecutor(String logicalQueueName, Message message, QueueAttributes queueAttributes) {
			this.logicalQueueName = logicalQueueName;
			this.message = message;
			this.queueUrl = queueAttributes.getReceiveMessageRequest().getQueueUrl();
			this.hasRedrivePolicy = queueAttributes.hasRedrivePolicy();
			this.deletionPolicy = queueAttributes.getDeletionPolicy();
		}

		@Override
		public void run() {
			String receiptHandle = this.message.getReceiptHandle();
			org.springframework.messaging.Message<String> queueMessage = getMessageForExecution();
			try {
				executeMessage(queueMessage);
				applyDeletionPolicyOnSuccess(receiptHandle);
			} catch (MessagingException messagingException) {
				applyDeletionPolicyOnError(receiptHandle, messagingException);
			}

			messageExecuted();
		}

		private void applyDeletionPolicyOnSuccess(String receiptHandle) {
			if (this.deletionPolicy == SqsMessageDeletionPolicy.ON_SUCCESS ||
					this.deletionPolicy == SqsMessageDeletionPolicy.ALWAYS ||
					this.deletionPolicy == SqsMessageDeletionPolicy.NO_REDRIVE) {
				deleteMessage(receiptHandle);
			}
		}

		private void applyDeletionPolicyOnError(String receiptHandle, MessagingException messagingException) {
			if (this.deletionPolicy == SqsMessageDeletionPolicy.ALWAYS ||
					(this.deletionPolicy == SqsMessageDeletionPolicy.NO_REDRIVE && !this.hasRedrivePolicy)) {
				deleteMessage(receiptHandle);
			} else if (this.deletionPolicy == SqsMessageDeletionPolicy.ON_SUCCESS) {
				getLogger().error("Exception encountered while processing message.", messagingException);
			}
		}

		private void deleteMessage(String receiptHandle) {
			getAmazonSqs().deleteMessageAsync(new DeleteMessageRequest(this.queueUrl, receiptHandle));
		}

		private org.springframework.messaging.Message<String> getMessageForExecution() {
			HashMap<String, Object> additionalHeaders = new HashMap<>();
			additionalHeaders.put(QueueMessageHandler.LOGICAL_RESOURCE_ID, this.logicalQueueName);
			if (this.deletionPolicy == SqsMessageDeletionPolicy.NEVER) {
				String receiptHandle = this.message.getReceiptHandle();
				QueueMessageAcknowledgment acknowledgment = new QueueMessageAcknowledgment(SimpleMessageListenerContainer.this.getAmazonSqs(), this.queueUrl, receiptHandle);
				additionalHeaders.put(QueueMessageHandler.ACKNOWLEDGMENT, acknowledgment);
			}

			return createMessage(this.message, additionalHeaders);
		}
	}

	private static class SignalExecutingRunnable implements Runnable {

		private final CountDownLatch countDownLatch;
		private final Runnable runnable;

		private SignalExecutingRunnable(CountDownLatch endSignal, Runnable runnable) {
			this.countDownLatch = endSignal;
			this.runnable = runnable;
		}

		@Override
		public void run() {
			this.countDownLatch.countDown();

			this.runnable.run();

		}
	}
}
