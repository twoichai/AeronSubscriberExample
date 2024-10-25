package com.example.aeronsubscriberexample.publisher;

import com.example.aeronsubscriberexample.config.SampleConfiguration;
import io.aeron.Aeron;
import io.aeron.Publication;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
public class BasicPublisher implements CommandLineRunner {
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;
    private static final long NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;
    private static final long LINGER_TIMEOUT_MS = SampleConfiguration.LINGER_TIMEOUT_MS;

    @Override
    public void run(String... args) throws Exception {
        final Aeron.Context ctx = new Aeron.Context();

        try (Aeron aeron = Aeron.connect(ctx);
             Publication publication = aeron.addPublication(CHANNEL, STREAM_ID)) {

            final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(256, 64));

            // Start the timer
            long startTime = System.currentTimeMillis();

            for (long i = 0; i < NUMBER_OF_MESSAGES; i++) {
                String uuid = UUID.randomUUID().toString();
                String publishingTimestamp = String.valueOf(Instant.now().toEpochMilli());
                String message = uuid + " " + publishingTimestamp;

                final int length = buffer.putStringWithoutLengthAscii(0, message);
                final long position = publication.offer(buffer, 0, length);

                if (position <= 0) {
                    handlePublicationError(position);

                    Thread.sleep(100);
                    i--;
                    continue;
                }
                Thread.sleep(1);
            }

            long endTime = System.currentTimeMillis();
            long totalTimeMillis = endTime - startTime;
            double totalTimeSeconds = totalTimeMillis / 1000.0;
            double throughput = NUMBER_OF_MESSAGES / totalTimeSeconds;

            System.out.println("Total UUID messages published: " + NUMBER_OF_MESSAGES);
            System.out.println("Total time: " + totalTimeSeconds + " seconds");
            System.out.println("Average throughput: " + throughput + " messages/second");

            if (LINGER_TIMEOUT_MS > 0) {
                System.out.println("Lingering for " + LINGER_TIMEOUT_MS + " milliseconds...");
                Thread.sleep(LINGER_TIMEOUT_MS);
            }
        }
    }

    private void handlePublicationError(long position) {
        if (position == Publication.BACK_PRESSURED) {
            System.out.println("Offer failed due to back pressure");
        } else if (position == Publication.NOT_CONNECTED) {
            System.out.println("Offer failed because publisher is not connected to a subscriber");
        } else if (position == Publication.ADMIN_ACTION) {
            System.out.println("Offer failed because of an administration action in the system");
        } else if (position == Publication.CLOSED) {
            System.out.println("Offer failed because publication is closed");
        } else if (position == Publication.MAX_POSITION_EXCEEDED) {
            System.out.println("Offer failed due to publication reaching its max position");
        } else {
            System.out.println("Offer failed due to unknown reason: " + position);
        }
    }
}