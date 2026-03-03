package com.sats.domain.strategy;

import com.sats.domain.enums.TargetType;
import com.sats.domain.model.BatchPayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class JsonWriteStrategy implements WriteStrategy {

    @Override
    public TargetType supports() {
        return TargetType.JSON;
    }

    @Override
    public void write(BatchPayload payload) {
        log.info(
                "Writing batch {} ({} records) to JSON target for dataset {}",
                payload.batchId(), payload.records().size(), payload.datasetId()
        );
        // TODO: Convert to Dataset<Row>, write with format("json").mode("append")
    }
}
