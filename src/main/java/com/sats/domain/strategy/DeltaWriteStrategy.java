package com.sats.domain.strategy;

import com.sats.config.SatsProperties;
import com.sats.domain.enums.TargetType;
import com.sats.domain.model.BatchPayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class DeltaWriteStrategy implements WriteStrategy {

    private final SatsProperties satsProperties;

    @Override
    public TargetType supports() {
        return TargetType.DELTA;
    }

    @Override
    public void write(BatchPayload payload) {
        log.info(
                "Writing batch {} ({} records, {} bytes) to Delta target for dataset {}",
                payload.batchId(), payload.records().size(),
                payload.batchSizeBytes(), payload.datasetId()
        );
        // TODO: Convert payload.records() to Dataset<Row> via SparkSessionManager,
        //       then write with format("delta").mode("append").option("mergeSchema", mergeSchema)
    }
}
