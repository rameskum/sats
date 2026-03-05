package com.sats.consumer;

import com.sats.config.SatsProperties;
import com.sats.domain.enums.MessageType;
import com.sats.store.RawMessageStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Thin Kafka batch listener: persists every raw message, resolves the topic
 * descriptor, then delegates to {@link RecordProcessor} for the full
 * decode → transform → accumulate pipeline.
 *
 * <p>All processing logic lives in {@link RecordProcessor} so the same pipeline
 * can be exercised by the replay path without duplicating code.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SatsKafkaListener implements BatchAcknowledgingMessageListener<String, byte[]> {

    private final TopicRegistry topicRegistry;
    private final RawMessageStore rawMessageStore;
    private final RecordProcessor recordProcessor;

    @Override
    public void onMessage(List<ConsumerRecord<String, byte[]>> records, Acknowledgment ack) {
        log.debug("Received batch of {} records", records.size());
        for (var record : records) {
            var descriptor = topicRegistry.lookup(record.topic())
                    .orElseGet(() -> {
                        log.warn("No descriptor for topic '{}' — treating as PLAIN_TEXT", record.topic());
                        return new SatsProperties.ConsumerConfig.TopicDescriptor(
                                record.topic(), MessageType.PLAIN_TEXT, record.topic());
                    });

            // Persist raw bytes before any transformation — captures even parse failures
            rawMessageStore.ingest(record, descriptor.type());

            recordProcessor.process(
                    record.value(), descriptor.type(),
                    record.topic(), record.partition(), record.offset(), record.key());
        }
        ack.acknowledge();
    }
}
