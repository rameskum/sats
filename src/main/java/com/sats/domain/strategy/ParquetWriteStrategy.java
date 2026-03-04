package com.sats.domain.strategy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sats.config.SatsProperties;
import com.sats.domain.enums.TargetType;
import com.sats.domain.model.BatchPayload;
import com.sats.service.RecordStandardizer;
import com.sats.writer.SparkSessionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
@Slf4j
public class ParquetWriteStrategy implements WriteStrategy {

    private final SatsProperties satsProperties;
    private final SparkSessionManager sparkSessionManager;
    private final RecordStandardizer recordStandardizer;
    private final ObjectMapper objectMapper;

    @Override
    public TargetType supports() {
        return TargetType.PARQUET;
    }

    @Override
    public void write(BatchPayload payload) {
        log.info("Writing batch {} ({} records) to Parquet target for dataset {}",
                payload.batchId(), payload.records().size(), payload.datasetId());

        String outputPath = satsProperties.spark().outputBasePath() + "/" + payload.datasetId();

        SparkSession spark;
        try {
            spark = sparkSessionManager.getSession();
        } catch (Exception e) {
            sparkSessionManager.invalidateAndRefresh();
            spark = sparkSessionManager.getSession();
        }

        Dataset<Row> df = toDataFrame(spark, payload);

        df.write()
                .format("parquet")
                .mode("append")
                .save(outputPath);

        log.info("Parquet write complete: dataset={}, path={}, records={}",
                payload.datasetId(), outputPath, payload.records().size());
    }

    private Dataset<Row> toDataFrame(SparkSession spark, BatchPayload payload) {
        List<String> jsonRows = payload.records().stream()
                .map(record -> {
                    try {
                        return objectMapper.writeValueAsString(recordStandardizer.standardize(record));
                    } catch (Exception e) {
                        throw new IllegalStateException("JSON serialisation failed for record", e);
                    }
                })
                .collect(Collectors.toList());

        return spark.read().json(spark.createDataset(jsonRows, Encoders.STRING()));
    }
}
