package com.periscope.cdc;

import com.periscope.model.ChangeEvent;
import com.periscope.model.OperationType;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses raw text messages emitted by PostgreSQL's 'test_decoding' logical decoding plugin
 * into strongly-typed ChangeEvent records.
 *
 * Example test_decoding message formats:
 * - BEGIN 684
 * - COMMIT 684
 * - table public.customers: INSERT: id[integer]:1 name[text]:'Alice' email[text]:'alice@example.com'
 * - table public.customers: UPDATE: id[integer]:1 name[text]:'Bob'
 * - table public.customers: DELETE: id[integer]:1
 */
public class WalMessageParser {

    /**
     * Regex matching the row-level change envelope:
     * Group 1: schema (e.g. "public")
     * Group 2: table (e.g. "customers")
     * Group 3: operation (e.g. "INSERT", "UPDATE", "DELETE", "TRUNCATE")
     * Group 4: column data string (e.g. "id[integer]:1 name[text]:'Alice'")
     */
    private static final Pattern ROW_PATTERN = Pattern.compile(
            "^table\\s+([a-zA-Z0-9_]+)\\.([a-zA-Z0-9_]+):\\s+(INSERT|UPDATE|DELETE|TRUNCATE):\\s*(.*)$"
    );

    /**
     * Regex matching an individual column tuple:
     * Group 1: column name (e.g. "id", "name")
     * Group 2: data type (e.g. "integer", "text", "boolean")
     * Group 3: quoted string value without single quotes (e.g. 'Alice' -> "Alice")
     * Group 4: unquoted value (e.g. "1", "true", "null")
     */
    private static final Pattern COLUMN_PATTERN = Pattern.compile(
            "([a-zA-Z0-9_]+)\\[([^\\]]+)\\]:(?:'([^']*)'|(\\S+))"
    );

    /**
     * Parses a raw text message from the WAL replication stream into an Optional ChangeEvent.
     * Returns Optional.empty() for non-row events such as BEGIN or COMMIT transactions.
     *
     * @param rawMessage the raw string line received from test_decoding
     * @param lsn        the Log Sequence Number associated with this WAL message
     * @return an Optional containing the parsed ChangeEvent, or Optional.empty() if not a row change
     */
    public Optional<ChangeEvent> parse(String rawMessage, long lsn) {
        if (rawMessage == null || rawMessage.isBlank()) {
            return Optional.empty();
        }

        if (rawMessage.startsWith("BEGIN") || rawMessage.startsWith("COMMIT")) {
            return Optional.empty();
        }

        Matcher matcher = ROW_PATTERN.matcher(rawMessage);
        if (!matcher.matches()) {
            return Optional.empty();
        }

        String schemaName = matcher.group(1);
        String tableName = matcher.group(2);
        String operationStr = matcher.group(3);
        String columnsRaw = matcher.group(4);
        OperationType operationType = OperationType.valueOf(operationStr);

        Map<String, Object> columns = parseColumns(columnsRaw);

        Map<String, Object> before = null;
        Map<String, Object> after = null;

        switch (operationType) {
            case INSERT:
                before = null;
                after = columns;
                break;
            case UPDATE:
                before = null;
                after = columns;
                break;
            case DELETE:
                before = columns;
                after = null;
                break;
            case TRUNCATE:
                before = null;
                after = null;
                break;
        }

        String primaryKeyColumn = null;
        if (columns.containsKey("id")) {
            primaryKeyColumn = "id";
        } else if (!columns.isEmpty()) {
            primaryKeyColumn = columns.keySet().iterator().next();
        }

        return Optional.of(new ChangeEvent(schemaName, tableName, operationType, lsn, Instant.now(), before, after, primaryKeyColumn));
    }

    /**
     * Parses column string into a map of column names to typed values.
     * Example input: "id[integer]:1 name[text]:'Alice' active[boolean]:true"
     */
    Map<String, Object> parseColumns(String columnsRaw) {
        Map<String, Object> columns = new LinkedHashMap<>();
        if (columnsRaw == null || columnsRaw.isBlank()) {
            return columns;
        }

        Matcher matcher = COLUMN_PATTERN.matcher(columnsRaw);
        while (matcher.find()) {
            String columnName = matcher.group(1);
            String dataType = matcher.group(2);

            String rawValue = (matcher.group(3) != null) ? matcher.group(3) : matcher.group(4);

            Object typedValue = castValue(dataType, rawValue);
            columns.put(columnName, typedValue);
        }
        return columns;
    }

    /**
     * Converts raw text representation to a typed Java Object based on PostgreSQL data type.
     */
    Object castValue(String dataType, String rawValue) {
        if (rawValue == null || "null".equalsIgnoreCase(rawValue)) {
            return null;
        }
        return switch (dataType) {
            case "integer", "int", "int4", "smallint", "int2" -> Integer.parseInt(rawValue);
            case "bigint", "int8" -> Long.parseLong(rawValue);
            case "boolean", "bool" -> Boolean.parseBoolean(rawValue);
            case "numeric", "decimal", "float4", "float8", "double precision" -> Double.parseDouble(rawValue);
            default -> rawValue;
        };

    }
}
