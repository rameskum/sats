package com.sats.service;

import com.sats.domain.enums.DataType;
import com.sats.domain.model.FieldSpec;
import com.sats.domain.model.RescueEntry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

/**
 * Performs safe-cast type validation using Java 21 pattern matching (Section 4.3).
 * Returns the coerced value on success or a {@link RescueEntry} on failure.
 */
@Service
@Slf4j
public class SchemaValidator {

    /**
     * Attempts to cast {@code rawValue} to the type declared in {@code spec}.
     *
     * @return An {@link Optional} containing the cast value, or empty if rescue is required.
     */
    public Optional<Object> safeCast(Object rawValue, FieldSpec spec) {
        if (rawValue == null) {
            return spec.nullable() ? Optional.empty() : Optional.empty();
        }
        return switch (spec.targetType()) {
            case STRING  -> Optional.of(rawValue.toString());
            case DOUBLE  -> toDouble(rawValue);
            case LONG    -> toLong(rawValue);
            case INTEGER -> toInteger(rawValue);
            case BOOLEAN -> toBoolean(rawValue);
            case TIMESTAMP -> toTimestamp(rawValue);
            default -> {
                log.warn("No safe-cast path for target type {} on field {}",
                        spec.targetType(), spec.fieldName());
                yield Optional.empty();
            }
        };
    }

    public RescueEntry rescue(String fieldName, Object rawValue, DataType expected, String reason) {
        return new RescueEntry(fieldName, rawValue, expected, reason);
    }

    // ── Private cast helpers ──────────────────────────────────────────

    private Optional<Object> toDouble(Object raw) {
        return switch (raw) {
            case Double d   -> Optional.of(d);
            case Number n   -> Optional.of(n.doubleValue());
            case String s   -> parseDouble(s);
            default         -> Optional.empty();
        };
    }

    private Optional<Object> toLong(Object raw) {
        return switch (raw) {
            case Long l     -> Optional.of(l);
            case Integer i  -> Optional.of((long) i);
            case String s   -> parseLong(s);
            default         -> Optional.empty();
        };
    }

    private Optional<Object> toInteger(Object raw) {
        return switch (raw) {
            case Integer i  -> Optional.of(i);
            case Long l when l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE
                            -> Optional.of(l.intValue());
            case String s   -> parseInt(s);
            default         -> Optional.empty();
        };
    }

    private Optional<Object> toBoolean(Object raw) {
        return switch (raw) {
            case Boolean b  -> Optional.of(b);
            case String s when "true".equalsIgnoreCase(s)  -> Optional.of(true);
            case String s when "false".equalsIgnoreCase(s) -> Optional.of(false);
            default         -> Optional.empty();
        };
    }

    private Optional<Object> toTimestamp(Object raw) {
        return switch (raw) {
            case Timestamp t -> Optional.of(t);
            case Long epoch  -> Optional.of(Timestamp.from(Instant.ofEpochMilli(epoch)));
            case String s    -> parseTimestamp(s);
            default          -> Optional.empty();
        };
    }

    private Optional<Object> parseDouble(String s) {
        try { return Optional.of(Double.parseDouble(s)); }
        catch (NumberFormatException e) { return Optional.empty(); }
    }

    private Optional<Object> parseLong(String s) {
        try { return Optional.of(Long.parseLong(s)); }
        catch (NumberFormatException e) { return Optional.empty(); }
    }

    private Optional<Object> parseInt(String s) {
        try { return Optional.of(Integer.parseInt(s)); }
        catch (NumberFormatException e) { return Optional.empty(); }
    }

    private Optional<Object> parseTimestamp(String s) {
        try { return Optional.of(Timestamp.from(Instant.parse(s))); }
        catch (Exception e) { return Optional.empty(); }
    }
}
