package com.sats.replay;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST endpoint for triggering a replay of raw Kafka messages from the
 * {@code __raw_messages__} Delta table.
 *
 * <pre>
 * POST /api/replay
 * {
 *   "topic":       "orders",
 *   "partition":   0,            // omit to replay all partitions
 *   "startOffset": 1000,
 *   "endOffset":   2000,         // omit for no upper bound
 *   "messageType": "DATA_RECORD",// omit to replay all types
 *   "limit":       500           // default 1000; safety cap
 * }
 * </pre>
 *
 * The call blocks until replay completes and returns a {@link ReplayResult}
 * with success/failure counts and sampled error messages.
 */
@RestController
@RequestMapping("/api/replay")
@RequiredArgsConstructor
@Slf4j
public class ReplayController {

    private final ReplayService replayService;

    @PostMapping
    public ResponseEntity<ReplayResult> replay(@RequestBody ReplayRequest request) {
        try {
            var result = replayService.replay(request).get();
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("Replay request failed: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(new ReplayResult(0, 0, 0, java.util.List.of(e.getMessage())));
        }
    }
}
