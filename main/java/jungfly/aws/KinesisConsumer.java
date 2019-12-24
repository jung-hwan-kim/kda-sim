package jungfly.aws;



import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class KinesisConsumer {

    private static final Logger log = LoggerFactory.getLogger(KinesisConsumer.class);


    private final String streamName;
    private final Region region;
    private final KinesisAsyncClient kinesisClient;

    private class ProcessorFactory implements ShardRecordProcessorFactory {
        private KinesisListener listener;
        public ProcessorFactory(KinesisListener listener) {
            this.listener = listener;
        }
        public ShardRecordProcessor shardRecordProcessor() {
            return new ShardRecordProcessorImpl(listener);
        }
    }

    private ShardRecordProcessorFactory processorFactory;
    public KinesisConsumer(String streamName, String region, KinesisListener listener) {
        this.streamName = streamName;
        this.region = Region.of(ObjectUtils.firstNonNull(region, "us-east-1"));
        this.kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(this.region));
        this.processorFactory = new ProcessorFactory(listener);
    }

    public void run() {
        //ScheduledExecutorService producerExecutor = Executors.newSingleThreadScheduledExecutor();
        //ScheduledFuture<?> producerFuture = producerExecutor.scheduleAtFixedRate(this::publishRecord, 10, 1, TimeUnit.SECONDS);

        DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(region).build();
        CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(region).build();
        ConfigsBuilder configsBuilder = new ConfigsBuilder(streamName, streamName, kinesisClient, dynamoClient, cloudWatchClient, UUID.randomUUID().toString(), processorFactory);

        Scheduler scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig().retrievalSpecificConfig(new PollingConfig(streamName, kinesisClient))
        );

        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.setDaemon(true);
        schedulerThread.start();

        System.out.println("Press enter to shutdown");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        try {
            reader.readLine();
        } catch (IOException ioex) {
            log.error("Caught exception while waiting for confirm. Shutting down.", ioex);
        }

        log.info("Cancelling producer and shutting down executor.");
        //producerFuture.cancel(true);
        //producerExecutor.shutdownNow();

        Future<Boolean> gracefulShutdownFuture = scheduler.startGracefulShutdown();
        log.info("Waiting up to 20 seconds for shutdown to complete.");
        try {
            gracefulShutdownFuture.get(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.info("Interrupted while waiting for graceful shutdown. Continuing.");
        } catch (ExecutionException e) {
            log.error("Exception while executing graceful shutdown.", e);
        } catch (TimeoutException e) {
            log.error("Timeout while waiting for shutdown.  Scheduler may not have exited.");
        }
        log.info("Completed, shutting down now.");
    }



    private static class ShardRecordProcessorImpl implements ShardRecordProcessor {

        private static final Logger log = LoggerFactory.getLogger(ShardRecordProcessorImpl.class);

        private String shardId;
        private KinesisListener listener;
        public ShardRecordProcessorImpl(KinesisListener listener) {
            this.listener = listener;
        }

        public void initialize(InitializationInput initializationInput) {
            shardId = initializationInput.shardId();
            try {
                log.info("Initializing @ Sequence: {}", initializationInput.extendedSequenceNumber());
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }

        public void processRecords(ProcessRecordsInput processRecordsInput) {
            try {
                log.debug("Processing {} record(s)", processRecordsInput.records().size());
                processRecordsInput.records().forEach(r -> {
                    CharBuffer charBuffer = StandardCharsets.US_ASCII.decode(r.data());
                    listener.listen(shardId, r.partitionKey(), r.sequenceNumber(), charBuffer.toString());
                });
            } catch (Throwable t) {
                t.printStackTrace();
                log.error("Caught throwable while processing records. Aborting.");
                Runtime.getRuntime().halt(1);
            }
        }

        public void leaseLost(LeaseLostInput leaseLostInput) {
            log.info("leaseLost");
        }

        public void shardEnded(ShardEndedInput shardEndedInput) {
            try {
                log.info("Reached shard end checkpointing.");
                shardEndedInput.checkpointer().checkpoint();
            } catch (ShutdownException | InvalidStateException e) {
                log.error("Exception while checkpointing at shard end. Giving up.", e);
            }
        }

        public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
            try {
                log.info("Scheduler is shutting down, checkpointing.");
                shutdownRequestedInput.checkpointer().checkpoint();
            } catch (ShutdownException | InvalidStateException e) {
                log.error("Exception while checkpointing at requested shutdown. Giving up.", e);
            }
        }
    }
}
