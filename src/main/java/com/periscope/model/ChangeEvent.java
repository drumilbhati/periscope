package com.periscope.model;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public record ChangeEvent(String schemaName, String tableName, OperationType operation, long lsn, Instant timestamp, Map<String, Object> before, Map<String, Object> after, String primaryKeyColumn) {
    public ChangeEvent {
        before = (before != null) ? Collections.unmodifiableMap(new LinkedHashMap<>(before)) : null;
        after = (after != null) ? Collections.unmodifiableMap(new LinkedHashMap<>(after)) : null;
        if (timestamp == null) {
            timestamp = Instant.now();
        }
    }

    public Object getPrimaryKeyValue() {
        if (primaryKeyColumn == null || primaryKeyColumn.isBlank()) {
            return null;
        }
        if (after != null && after.containsKey(primaryKeyColumn)) {
            return after.get(primaryKeyColumn);
        }
        if (before != null && before.containsKey(primaryKeyColumn)) {
            return before.get(primaryKeyColumn);
        }
        return null;
    }
}
