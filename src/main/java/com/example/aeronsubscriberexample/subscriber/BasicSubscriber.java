package com.example.aeronsubscriberexample.subscriber;

import com.example.aeronsubscriberexample.config.SampleConfiguration;
import com.example.aeronsubscriberexample.utils.SamplesUtil;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.SigInt;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class BasicSubscriber implements CommandLineRunner {
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

    @Override
    public void run(String... args) throws Exception {
        System.out.println("Subscribing to " + CHANNEL + " on stream id " + STREAM_ID);

        final Aeron.Context ctx = new Aeron.Context()
                .availableImageHandler(SamplesUtil::printAvailableImage)
                .unavailableImageHandler(SamplesUtil::printUnavailableImage);

        SamplesUtil samplesUtil = new SamplesUtil();
        final FragmentHandler fragmentHandler = samplesUtil.printAsciiMessage(STREAM_ID);
        final AtomicBoolean running = new AtomicBoolean(true);

        // Register SIGINT handler to stop the subscriber
        SigInt.register(() -> {
            running.set(false);
            samplesUtil.calculateAndPrintStatistics(); // Print statistics upon shutdown
        });

        try (Aeron aeron = Aeron.connect(ctx);
             Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID)) {
            SamplesUtil.subscriberLoop(fragmentHandler, FRAGMENT_COUNT_LIMIT, running).accept(subscription);
            System.out.println("Shutting down...");
        }
    }
}