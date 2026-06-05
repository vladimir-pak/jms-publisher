package com.gpb.mkd.jms.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@RequiredArgsConstructor
public class JmsLoadMetrics {

    private final MeterRegistry meterRegistry;

    private final AtomicBoolean gaugesRegistered = new AtomicBoolean(false);

    private Counter sentCounter;
    private Counter successCounter;
    private Counter failedCounter;
    private Counter timeoutCounter;
    private Counter sendFailedCounter;
    private Counter replyReceivedCounter;
    private Counter replyWithoutPendingCounter;
    private Counter earlyReplyMatchedCounter;

    private Timer latencyTimer;

    private DistributionSummary payloadSizeSummary;
    private DistributionSummary responseSizeSummary;

    /**
     * Registers gauges bound to service state.
     * Must be called once during service startup after maps/counters are initialized.
     */
    public void registerGauges(
            AtomicInteger inFlight,
            ConcurrentMap<?, ?> pendingRequests,
            ConcurrentMap<?, ?> earlyReplies
    ) {
        initMetersIfNecessary();

        if (!gaugesRegistered.compareAndSet(false, true)) {
            return;
        }

        meterRegistry.gauge("mq.load.inflight", inFlight);
        meterRegistry.gauge("mq.load.pending", pendingRequests, ConcurrentMap::size);
        meterRegistry.gauge("mq.load.early_replies", earlyReplies, ConcurrentMap::size);
    }

    public void incrementInFlight(AtomicInteger inFlight) {
        inFlight.incrementAndGet();
    }

    public void decrementInFlight(AtomicInteger inFlight) {
        inFlight.decrementAndGet();
    }

    public void recordPayloadSize(String payload) {
        initMetersIfNecessary();

        if (payload != null) {
            payloadSizeSummary.record(payload.getBytes(StandardCharsets.UTF_8).length);
        }
    }

    public void recordResponseSize(String responseBody) {
        initMetersIfNecessary();

        if (responseBody != null) {
            responseSizeSummary.record(responseBody.getBytes(StandardCharsets.UTF_8).length);
        }
    }

    public void incrementSent() {
        initMetersIfNecessary();
        sentCounter.increment();
    }

    public void incrementFailed() {
        initMetersIfNecessary();
        failedCounter.increment();
    }

    public void incrementTimeout() {
        initMetersIfNecessary();
        timeoutCounter.increment();
    }

    public void incrementSendFailed() {
        initMetersIfNecessary();
        sendFailedCounter.increment();
    }

    public void incrementReplyReceived() {
        initMetersIfNecessary();
        replyReceivedCounter.increment();
    }

    public void incrementReplyWithoutPending() {
        initMetersIfNecessary();
        replyWithoutPendingCounter.increment();
    }

    public void incrementEarlyReplyMatched() {
        initMetersIfNecessary();
        earlyReplyMatchedCounter.increment();
    }

    public void recordSuccess(long durationMs, String responseBody) {
        initMetersIfNecessary();

        successCounter.increment();
        latencyTimer.record(Duration.ofMillis(durationMs));
        recordResponseSize(responseBody);
    }

    public void recordSendFailure() {
        incrementSendFailed();
        incrementFailed();
    }

    public void recordTimeoutFailure() {
        incrementTimeout();
        incrementFailed();
    }

    private void initMetersIfNecessary() {
        if (sentCounter != null) {
            return;
        }

        synchronized (this) {
            if (sentCounter != null) {
                return;
            }

            this.sentCounter = Counter.builder("mq.load.sent.total")
                    .description("Total sent JMS request messages")
                    .register(meterRegistry);

            this.successCounter = Counter.builder("mq.load.success.total")
                    .description("Total successful request-response messages")
                    .register(meterRegistry);

            this.failedCounter = Counter.builder("mq.load.failed.total")
                    .description("Total failed request-response messages")
                    .register(meterRegistry);

            this.timeoutCounter = Counter.builder("mq.load.timeout.total")
                    .description("Total request-response timeouts")
                    .register(meterRegistry);

            this.sendFailedCounter = Counter.builder("mq.load.send.failed.total")
                    .description("Total send failures")
                    .register(meterRegistry);

            this.replyReceivedCounter = Counter.builder("mq.load.reply.received.total")
                    .description("Total received reply messages")
                    .register(meterRegistry);

            this.replyWithoutPendingCounter = Counter.builder("mq.load.reply.without_pending.total")
                    .description("Replies without pending request at receive time")
                    .register(meterRegistry);

            this.earlyReplyMatchedCounter = Counter.builder("mq.load.reply.early.matched.total")
                    .description("Early replies matched after pending request registration")
                    .register(meterRegistry);

            this.latencyTimer = Timer.builder("mq.load.latency")
                    .description("Request-response latency")
                    .publishPercentileHistogram()
                    .publishPercentiles(0.5, 0.95, 0.99)
                    .minimumExpectedValue(Duration.ofMillis(1))
                    .maximumExpectedValue(Duration.ofMinutes(5))
                    .register(meterRegistry);

            this.payloadSizeSummary = DistributionSummary.builder("mq.load.payload.size.bytes")
                    .description("Request payload size")
                    .baseUnit("bytes")
                    .register(meterRegistry);

            this.responseSizeSummary = DistributionSummary.builder("mq.load.response.size.bytes")
                    .description("Response payload size")
                    .baseUnit("bytes")
                    .register(meterRegistry);
        }
    }
}
