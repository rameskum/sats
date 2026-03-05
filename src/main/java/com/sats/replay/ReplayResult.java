package com.sats.replay;

import java.util.List;

/**
 * Summary of a completed replay operation.
 *
 * <ul>
 *   <li>{@code totalFound} — rows returned by the Delta table query.</li>
 *   <li>{@code succeeded} — records that were processed without error.</li>
 *   <li>{@code failed} — records that failed (DLQ-routed or exception).</li>
 *   <li>{@code errors} — brief error messages for the first few failures
 *       (capped at 20 to keep the response size manageable).</li>
 * </ul>
 */
public record ReplayResult(
        int totalFound,
        int succeeded,
        int failed,
        List<String> errors
) {}
