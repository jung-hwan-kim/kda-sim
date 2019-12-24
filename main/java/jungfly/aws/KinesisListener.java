package jungfly.aws;

public interface KinesisListener {
    void listen(String shardId, String partitionKey, String sequenceNumber, String data);
}
