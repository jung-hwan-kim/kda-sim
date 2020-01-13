package jungfly.aws;

import clojure.lang.Keyword;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class KinesisProducer {
    private KinesisClient kinesisClient;

    public KinesisProducer() {
        kinesisClient = KinesisClient.builder().build();
    }
    public Integer _putRecords(String streamName, List<Map<Keyword, String>> records) {
        Keyword partitionKey = Keyword.find("PartitionKey");
        Keyword jsonStr = Keyword.find("Data");
        for (Map<Keyword, String> record : records) {
            String pKey = record.get(partitionKey);
            String data = record.get(jsonStr);
            System.out.println(pKey +  ":" + data);

        }
        return 0;
    }
    public Integer putRecords(String streamName, List<Map<Keyword, String>> records) {
        Keyword partitionKey = Keyword.find("PartitionKey");
        Keyword jsonStr = Keyword.find("Data");
        List<PutRecordsRequestEntry> list = new LinkedList<>();
        for (Map<Keyword, String> record : records) {
            String partionKey = String.valueOf(record.get(partitionKey));
            SdkBytes sdkBytes = SdkBytes.fromByteArray(record.get(jsonStr).getBytes());
            PutRecordsRequestEntry entry = PutRecordsRequestEntry.builder()
                                                                    .data(sdkBytes)
                                                                    .partitionKey(partionKey)
                                                                    .build();
            list.add(entry);
        }
        PutRecordsRequest putRecordsRequest = PutRecordsRequest.builder()
                                                                .records(list)
                                                                .streamName(streamName)
                                                                .build();
        PutRecordsResponse response = kinesisClient.putRecords(putRecordsRequest);
        return response.failedRecordCount();
    }
}