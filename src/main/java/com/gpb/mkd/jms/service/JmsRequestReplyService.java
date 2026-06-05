package com.gpb.mkd.jms.service;

import com.gpb.mkd.jms.config.ArtemisClientProperties;
import com.gpb.mkd.jms.dto.LoadRequest;
import com.gpb.mkd.jms.dto.LoadResponse;
import com.gpb.mkd.jms.dto.MessageResult;
import com.gpb.mkd.jms.metrics.JmsLoadMetrics;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@RequiredArgsConstructor
public class JmsRequestReplyService {

    private final ConnectionFactory connectionFactory;
    private final ArtemisClientProperties properties;
    private final JmsLoadMetrics metrics;

    /**
     * One long-lived connection for all sender sessions and the shared reply listener.
     */
    private Connection connection;

    /**
     * One dedicated listener session/consumer for shared reply queue.
     */
    private Session replySession;
    private MessageConsumer replyConsumer;

    /**
     * Sender pool. Each sender thread has its own JMS Session and MessageProducer.
     */
    private ExecutorService senderExecutor;
    private ScheduledExecutorService timeoutExecutor;

    private final ConcurrentMap<String, PendingRequest> pendingRequests = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ReceivedReply> earlyReplies = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<WorkerContext> workerContexts = new ConcurrentLinkedQueue<>();

    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger workerNumber = new AtomicInteger();
    private final AtomicInteger timeoutNumber = new AtomicInteger();

    private ThreadLocal<WorkerContext> workerContext;

    @PostConstruct
    public void start() throws JMSException {
        metrics.registerGauges(inFlight, pendingRequests, earlyReplies);

        this.connection = connectionFactory.createConnection();

        this.replySession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue replyQueue = replySession.createQueue(properties.getReplyQueue());
        this.replyConsumer = replySession.createConsumer(replyQueue);
        this.replyConsumer.setMessageListener(this::onReplyMessage);

        this.senderExecutor = Executors.newFixedThreadPool(
                properties.getConcurrentSenders(),
                namedThreadFactory("jms-sender-", workerNumber)
        );

        this.timeoutExecutor = Executors.newScheduledThreadPool(
                2,
                namedThreadFactory("jms-timeout-", timeoutNumber)
        );

        this.workerContext = ThreadLocal.withInitial(this::createWorkerContextUnchecked);

        this.connection.start();

        this.timeoutExecutor.scheduleAtFixedRate(
                this::cleanupEarlyRepliesSafely,
                properties.getEarlyReplyTtlMs(),
                properties.getEarlyReplyTtlMs(),
                TimeUnit.MILLISECONDS
        );

        log.info("JMS optimized request-reply client started. brokerUrl={}, requestQueue={}, replyQueue={}, senderThreads={}",
                properties.getBrokerUrl(),
                properties.getRequestQueue(),
                properties.getReplyQueue(),
                properties.getConcurrentSenders());
    }

    public LoadResponse runLoadTest(LoadRequest request) {
        int effectiveConcurrency = resolveConcurrency(request);
        Semaphore concurrencyLimiter = new Semaphore(effectiveConcurrency);

        long started = System.nanoTime();
        List<CompletableFuture<MessageResult>> tasks = new ArrayList<>(request.getCount());

        for (int i = 0; i < request.getCount(); i++) {
            int index = i + 1;
            tasks.add(CompletableFuture.supplyAsync(
                    () -> sendOne(index, request, concurrencyLimiter),
                    senderExecutor
            ));
        }

        List<MessageResult> results = tasks.stream()
                .map(CompletableFuture::join)
                .toList();

        long totalDurationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        int success = (int) results.stream().filter(MessageResult::isSuccess).count();
        int failed = results.size() - success;

        List<Long> latencies = results.stream()
                .filter(MessageResult::isSuccess)
                .map(MessageResult::getDurationMs)
                .sorted()
                .toList();

        double avgLatencyMs = latencies.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0);

        long minLatencyMs = latencies.stream()
                .min(Comparator.naturalOrder())
                .orElse(0L);

        long maxLatencyMs = latencies.stream()
                .max(Comparator.naturalOrder())
                .orElse(0L);

        double requestsPerSecond = totalDurationMs > 0
                ? success * 1000.0 / totalDurationMs
                : success;

        return LoadResponse.builder()
                .requested(request.getCount())
                .success(success)
                .failed(failed)
                .totalDurationMs(totalDurationMs)
                .requestsPerSecond(requestsPerSecond)
                .avgLatencyMs(avgLatencyMs)
                .minLatencyMs(minLatencyMs)
                .maxLatencyMs(maxLatencyMs)
                .results(request.isIncludeResponses() ? results : List.of())
                .build();
    }

    private MessageResult sendOne(int index, LoadRequest request, Semaphore concurrencyLimiter) {
        boolean acquired = false;
        try {
            concurrencyLimiter.acquire();
            acquired = true;
            return doSendOne(index, request);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            metrics.incrementFailed();
            return MessageResult.builder()
                    .index(index)
                    .success(false)
                    .error("Interrupted while waiting for concurrency permit")
                    .build();
        } finally {
            if (acquired) {
                concurrencyLimiter.release();
            }
        }
    }

    private MessageResult doSendOne(int index, LoadRequest request) {
        long started = System.nanoTime();
        metrics.incrementInFlight(inFlight);

        CompletableFuture<MessageResult> responseFuture = new CompletableFuture<>();
        String requestMessageId = null;
        String requestCorrelationId = null;

        try {
            WorkerContext context = workerContext.get();

            TextMessage message = context.session.createTextMessage(request.getPayload());
            applyRequestReplyHeaders(message, context.replyQueue);
            applyUserProperties(message, request.getProperties());
            applyIbmMqCompatibilityProperties(message);

            if (properties.getIbmMqCompatibility().isRequestCorrelationIdEnabled()) {
                requestCorrelationId = properties.getIbmMqCompatibility().getRequestCorrelationIdPrefix() + UUID.randomUUID();
                message.setJMSCorrelationID(requestCorrelationId);
            }

            metrics.recordPayloadSize(request.getPayload());

            context.producer.send(message);
            metrics.incrementSent();

            requestMessageId = message.getJMSMessageID();

            String expectedResponseCorrelationId = requestCorrelationId != null
                    ? requestCorrelationId
                    : requestMessageId;

            PendingRequest pending = new PendingRequest(
                    index,
                    started,
                    request.isIncludeResponses(),
                    expectedResponseCorrelationId,
                    requestMessageId,
                    responseFuture
            );

            pendingRequests.put(expectedResponseCorrelationId, pending);
            scheduleTimeout(pending);
            completeFromEarlyReplyIfExists(expectedResponseCorrelationId);

            return responseFuture.join();

        } catch (Exception e) {
            metrics.recordSendFailure();

            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            return MessageResult.builder()
                    .index(index)
                    .success(false)
                    .requestMessageId(requestMessageId)
                    .responseCorrelationId(requestCorrelationId)
                    .durationMs(durationMs)
                    .error(e.getMessage())
                    .build();
        } finally {
            metrics.decrementInFlight(inFlight);
        }
    }

    private void applyRequestReplyHeaders(TextMessage message, Destination replyQueue) throws JMSException {
        if (properties.getIbmMqCompatibility().isReplyToEnabled()) {
            message.setJMSReplyTo(replyQueue);
        }
    }

    private void applyUserProperties(TextMessage message, Map<String, String> userProperties) throws JMSException {
        if (userProperties == null || userProperties.isEmpty()) {
            return;
        }

        for (Map.Entry<String, String> entry : userProperties.entrySet()) {
            message.setStringProperty(entry.getKey(), entry.getValue());
        }
    }

    /**
     * These properties are meaningful mostly when the message is bridged to IBM MQ or sent through IBM MQ JMS.
     * For pure Artemis they are only message properties, so they are disabled by default.
     */
    private void applyIbmMqCompatibilityProperties(TextMessage message) throws JMSException {
        ArtemisClientProperties.IbmMqCompatibility mq = properties.getIbmMqCompatibility();
        if (!mq.isIbmPropertiesEnabled()) {
            return;
        }

        message.setIntProperty("JMS_IBM_MsgType", mq.getMqMessageType());
        message.setStringProperty("JMS_IBM_Format", mq.getMqFormat());
        message.setIntProperty("JMS_IBM_Character_Set", mq.getCharacterSet());
        message.setIntProperty("JMS_IBM_Encoding", mq.getEncoding());
    }

    private void scheduleTimeout(PendingRequest pending) {
        timeoutExecutor.schedule(() -> {
            PendingRequest removed = pendingRequests.remove(pending.expectedResponseCorrelationId());
            if (removed == null) {
                return;
            }

            metrics.recordTimeoutFailure();

            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - removed.startedNanos());
            removed.future().complete(MessageResult.builder()
                    .index(removed.index())
                    .success(false)
                    .requestMessageId(removed.requestMessageId())
                    .responseCorrelationId(removed.expectedResponseCorrelationId())
                    .durationMs(durationMs)
                    .error("Response timeout. expectedCorrelationId=" + removed.expectedResponseCorrelationId())
                    .build());
        }, properties.getReceiveTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    private void onReplyMessage(Message message) {
        try {
            metrics.incrementReplyReceived();

            String responseCorrelationId = message.getJMSCorrelationID();
            String responseBody = extractBody(message);

            if (responseCorrelationId == null || responseCorrelationId.isBlank()) {
                metrics.incrementReplyWithoutPending();
                log.warn("Received reply without JMSCorrelationID. message={}", message);
                return;
            }

            ReceivedReply reply = new ReceivedReply(responseCorrelationId, responseBody, System.nanoTime());
            PendingRequest pending = pendingRequests.remove(responseCorrelationId);

            if (pending != null) {
                completeSuccess(pending, reply);
                return;
            }

            if (earlyReplies.size() < properties.getEarlyReplyMaxSize()) {
                earlyReplies.put(responseCorrelationId, reply);
            }

            metrics.incrementReplyWithoutPending();
        } catch (Exception e) {
            metrics.incrementReplyWithoutPending();
            log.error("Failed to process reply message", e);
        }
    }

    private void completeFromEarlyReplyIfExists(String expectedCorrelationId) {
        ReceivedReply earlyReply = earlyReplies.remove(expectedCorrelationId);
        if (earlyReply == null) {
            return;
        }

        PendingRequest pending = pendingRequests.remove(expectedCorrelationId);
        if (pending == null) {
            return;
        }
        metrics.incrementEarlyReplyMatched();
        completeSuccess(pending, earlyReply);
    }

    private void completeSuccess(PendingRequest pending, ReceivedReply reply) {
        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - pending.startedNanos());

        metrics.recordSuccess(durationMs, reply.body());

        pending.future().complete(MessageResult.builder()
                .index(pending.index())
                .success(true)
                .requestMessageId(pending.requestMessageId())
                .responseCorrelationId(reply.correlationId())
                .responseBody(pending.includeResponse() ? reply.body() : null)
                .durationMs(durationMs)
                .build());
    }

    private String extractBody(Message message) throws JMSException {
        if (message instanceof TextMessage textMessage) {
            return textMessage.getText();
        }

        try {
            return message.getBody(String.class);
        } catch (Exception e) {
            return message.toString();
        }
    }

    private WorkerContext createWorkerContextUnchecked() {
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue requestQueue = session.createQueue(properties.getRequestQueue());
            Queue replyQueue = session.createQueue(properties.getReplyQueue());

            MessageProducer producer = session.createProducer(requestQueue);
            producer.setDeliveryMode(properties.isDeliveryModePersistent()
                    ? DeliveryMode.PERSISTENT
                    : DeliveryMode.NON_PERSISTENT);

            if (properties.getMessageTtlMs() > 0) {
                producer.setTimeToLive(properties.getMessageTtlMs());
            }

            WorkerContext context = new WorkerContext(session, producer, replyQueue);
            workerContexts.add(context);
            return context;
        } catch (JMSException e) {
            throw new IllegalStateException("Failed to create JMS worker context", e);
        }
    }

    private int resolveConcurrency(LoadRequest request) {
        int requested = request.getConcurrency() != null
                ? request.getConcurrency()
                : properties.getConcurrentSenders();

        return Math.max(1, Math.min(requested, properties.getConcurrentSenders()));
    }

    private void cleanupEarlyRepliesSafely() {
        try {
            long ttlNanos = TimeUnit.MILLISECONDS.toNanos(properties.getEarlyReplyTtlMs());
            long now = System.nanoTime();

            earlyReplies.entrySet().removeIf(entry -> now - entry.getValue().receivedNanos() > ttlNanos);
        } catch (Exception e) {
            log.warn("Failed to cleanup early replies", e);
        }
    }

    private ThreadFactory namedThreadFactory(String prefix, AtomicInteger counter) {
        return runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName(prefix + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    @PreDestroy
    public void shutdown() {
        log.info("Stopping JMS optimized request-reply client");

        if (senderExecutor != null) {
            senderExecutor.shutdownNow();
        }

        if (timeoutExecutor != null) {
            timeoutExecutor.shutdownNow();
        }

        for (WorkerContext context : workerContexts) {
            closeQuietly(context.producer());
            closeQuietly(context.session());
        }

        closeQuietly(replyConsumer);
        closeQuietly(replySession);
        closeQuietly(connection);
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }

        try {
            closeable.close();
        } catch (Exception e) {
            log.debug("Failed to close JMS resource", e);
        }
    }

    private record PendingRequest(
            int index,
            long startedNanos,
            boolean includeResponse,
            String expectedResponseCorrelationId,
            String requestMessageId,
            CompletableFuture<MessageResult> future
    ) {
    }

    private record ReceivedReply(
            String correlationId,
            String body,
            long receivedNanos
    ) {
    }

    private record WorkerContext(
            Session session,
            MessageProducer producer,
            Destination replyQueue
    ) {
    }
}
