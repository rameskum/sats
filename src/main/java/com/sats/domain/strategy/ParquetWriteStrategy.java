package com.sats.domain.strategy;

import com.sats.domain.enums.TargetType;
import com.sats.domain.model.BatchPayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class ParquetWriteStrategy implements WriteStrategy {

    @Override
    public TargetType supports() {
        return TargetType.PARQUET;
    }

    @Override
    public void write(BatchPayload payload) {
        log.info(
                "Writing batch {} ({} records) to Parquet target for dataset {}",
                payload.batchId(), payload.records().size(), payload.datasetId()
        );
        // TODO: Convert to Dataset<Row>, write with format("parquet").mode("append")
    }
}
