package com.example.aeronsubscriberexample.utils;

import com.example.aeronsubscriberexample.config.SampleConfiguration;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class SamplesUtil {

    private static final Logger log = LoggerFactory.getLogger(SamplesUtil.class);
    private ConcurrentLinkedQueue<String> latencies = new ConcurrentLinkedQueue<>();
    private int totalMessages = 0;  // Track total messages received
    private long startTime = 0;     // Track when the app started
    private long endTime = 0;       // Track when the app stopped

    public SamplesUtil() {
        startTime = System.currentTimeMillis();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::evaluateLatencies, 0, 1, TimeUnit.MINUTES);
    }

    public static Consumer<Subscription> subscriberLoop(
            final FragmentHandler fragmentHandler, final int limit, final AtomicBoolean running) {
        return subscriberLoop(fragmentHandler, limit, running, SampleConfiguration.newIdleStrategy());
    }

    public static Consumer<Subscription> subscriberLoop(
            final FragmentHandler fragmentHandler,
            final int limit,
            final AtomicBoolean running,
            final IdleStrategy idleStrategy) {
        return (subscription) -> {
            final FragmentAssembler assembler = new FragmentAssembler(fragmentHandler);
            while (running.get()) {
                final int fragmentsRead = subscription.poll(assembler, limit);
                idleStrategy.idle(fragmentsRead);
            }
        };
    }

    public FragmentHandler printAsciiMessage(final int streamId) {
        return (buffer, offset, length, header) -> {
            final String msg = buffer.getStringWithoutLengthAscii(offset, length);
            String[] parts = msg.split(" ");
            String publishingTimestampStr = parts[parts.length - 1]; // Last part should be the timestamp

            try {
                long publishingTimestamp = Long.parseLong(publishingTimestampStr);
                long receivingTimestamp = Instant.now().toEpochMilli();
                long latency = receivingTimestamp - publishingTimestamp;
                latencies.add(String.valueOf(latency));

                // Increment the total message counter
                totalMessages++;

                //System.out.printf("Message to stream %d from session %d (%d@%d) <<%s>> - Latency: %d ms%n",streamId, header.sessionId(), length, offset, msg, latency);
            } catch (NumberFormatException e) {
                //System.out.printf("Message to stream %d from session %d (%d@%d) <<%s>> - Unable to parse timestamp%n",streamId, header.sessionId(), length, offset, msg);
            }
        };
    }

    public void calculateAndPrintStatistics() {
        endTime = System.currentTimeMillis();
        double runtimeInSeconds = (endTime - startTime) / 1000.0;
        double throughput = totalMessages / runtimeInSeconds;

        log.info("Application runtime: {} seconds", runtimeInSeconds);
        log.info("Total messages received: {}", totalMessages);
        log.info("Throughput: {} messages/second", throughput);

        // Calculate and print latency statistics
        evaluateLatencies();
    }

    private void evaluateLatencies() {
        List<String> latenciesList = new ArrayList<>(latencies);
        if (!latenciesList.isEmpty()) {
            List<Long> latencyValues = new ArrayList<>();
            for (String latencyStr : latenciesList) {
                try {
                    latencyValues.add(Long.parseLong(latencyStr));
                } catch (NumberFormatException e) {
                    log.error("Error parsing latency: {}", e.getMessage());
                }
            }
            if (!latencyValues.isEmpty()) {
                long sum = latencyValues.stream().mapToLong(Long::longValue).sum();
                double avgLatency = sum / (double) latencyValues.size();
                long maxLatency = latencyValues.stream().mapToLong(Long::longValue).max().orElse(0L);

                latencyValues.sort(Long::compare);
                long p50 = latencyValues.get((int) (latencyValues.size() * 0.5));
                long p90 = latencyValues.get((int) (latencyValues.size() * 0.9));
                long p99 = latencyValues.get((int) (latencyValues.size() * 0.99));

                log.info("Latency Stats for the last minute:");
                log.info("Average Latency: {} ms", avgLatency);
                log.info("Max Latency: {} ms", maxLatency);
                log.info("50th Percentile (Median) Latency: {} ms", p50);
                log.info("90th Percentile Latency: {} ms", p90);
                log.info("99th Percentile Latency: {} ms", p99);
            }
            latencies.clear();
        } else {
            log.info("No latency data to evaluate in the last minute.");
        }
    }

    public static void printAvailableImage(final Image image) {
        final Subscription subscription = image.subscription();
        System.out.printf(
                "Available image on %s streamId=%d sessionId=%d mtu=%d term-length=%d from %s%n",
                subscription.channel(), subscription.streamId(), image.sessionId(), image.mtuLength(),
                image.termBufferLength(), image.sourceIdentity());
    }

    public static void printUnavailableImage(final Image image) {
        final Subscription subscription = image.subscription();
        System.out.printf(
                "Unavailable image on %s streamId=%d sessionId=%d%n",
                subscription.channel(), subscription.streamId(), image.sessionId());
    }
}