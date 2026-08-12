package com.gpb.mkd.jms.repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.mkd.jms.config.ComparisonProperties;
import com.gpb.mkd.jms.dto.CompareActionType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

@Slf4j
@Repository
@RequiredArgsConstructor
public class ComparisonRepository {

    private static final Pattern SAFE_IDENTIFIER =
            Pattern.compile("[A-Za-z0-9_.$]+", Pattern.CASE_INSENSITIVE);

    private final JdbcTemplate jdbcTemplate;
    private final ComparisonProperties properties;
    private final ObjectMapper objectMapper;

    /**
     * Новый Flink.
     *
     * Поиск:
     * event_id = ?
     * AND action_type = ANSWER / ANSWER_DETAIL
     */
    public Optional<JsonNode> findFlinkResult(
            String eventId,
            CompareActionType actionType
    ) {
        ComparisonProperties.Database db =
                properties.getFlink();

        validateFlinkDb(db);

        String sql =
                "SELECT " + id(db.getDataJsonColumn())
                        + " FROM " + id(db.getTable())
                        + " WHERE " + id(db.getEventIdColumn()) + " = ?"
                        + " AND " + id(db.getActionTypeColumn()) + " = ?"
                        + orderAndLimit(db);

        log.info(
                "[COMPARE-DB][FLINK] search eventId={} actionType={} table={}",
                eventId,
                actionType,
                db.getTable()
        );

        return queryJson(
                sql,
                eventId,
                actionType.name()
        );
    }

    /**
     * Старое Python-приложение.
     *
     * Первый шаг:
     *
     * action_type = QUERY
     * AND data_json ->> 'dfw_event_id' = eventId
     */
    public Optional<JsonNode> findLegacyQueryByEventId(
            String eventId
    ) {
        ComparisonProperties.Database db =
                properties.getLegacy();

        validateLegacyDb(db);

        String dataJson =
                id(db.getDataJsonColumn());

        String sql =
                "SELECT " + dataJson
                        + " FROM " + id(db.getTable())
                        + " WHERE " + id(db.getActionTypeColumn()) + " = 'QUERY'"
                        + " AND (" + dataJson + ")::jsonb ->> 'dfw_event_id' = ?"
                        + orderAndLimit(db);

        log.info(
                "[COMPARE-DB][LEGACY] search QUERY by dfw_event_id={} table={}",
                eventId,
                db.getTable()
        );

        return queryJson(
                sql,
                eventId
        );
    }

    /**
     * Старое Python-приложение.
     *
     * Второй шаг:
     *
     * из QUERY уже получили dfw_query_id.
     *
     * Теперь ищем:
     *
     * action_type = ANSWER / ANSWER_DETAIL
     * AND data_json ->> 'dfw_query_id' = queryId
     */
    public Optional<JsonNode> findLegacyResult(
            String queryId,
            CompareActionType actionType
    ) {
        ComparisonProperties.Database db =
                properties.getLegacy();

        validateLegacyDb(db);

        String dataJson =
                id(db.getDataJsonColumn());

        String sql =
                "SELECT " + dataJson
                        + " FROM " + id(db.getTable())
                        + " WHERE " + id(db.getActionTypeColumn()) + " = ?"
                        + " AND (" + dataJson + ")::jsonb ->> 'dfw_query_id' = ?"
                        + orderAndLimit(db);

        log.info(
                "[COMPARE-DB][LEGACY] search result queryId={} actionType={} table={}",
                queryId,
                actionType,
                db.getTable()
        );

        return queryJson(
                sql,
                actionType.name(),
                queryId
        );
    }

    /**
     * Выполняет SQL и возвращает первый data_json как JsonNode.
     */
    private Optional<JsonNode> queryJson(
            String sql,
            Object... args
    ) {
        try {
            List<String> rows =
                    jdbcTemplate.query(
                            sql,
                            (rs, rowNum) -> rs.getString(1),
                            args
                    );

            if (rows.isEmpty()) {
                log.info(
                        "[COMPARE-DB] query returned no rows"
                );

                return Optional.empty();
            }

            String json =
                    rows.get(0);

            if (json == null || json.isBlank()) {
                log.warn(
                        "[COMPARE-DB] query returned empty data_json"
                );

                return Optional.empty();
            }

            return Optional.of(
                    objectMapper.readTree(json)
            );

        } catch (Exception e) {

            log.error(
                    "[COMPARE-DB] query failed sql={}",
                    sql,
                    e
            );

            throw new IllegalStateException(
                    "Comparison DB query failed",
                    e
            );
        }
    }

    /**
     * Добавляет:
     *
     * ORDER BY id DESC
     * LIMIT 1
     *
     * Если order-column не задан:
     * LIMIT 1
     */
    private String orderAndLimit(
            ComparisonProperties.Database db
    ) {
        String orderColumn =
                db.getOrderColumn();

        if (orderColumn == null
                || orderColumn.isBlank()) {

            return " LIMIT 1";
        }

        return " ORDER BY "
                + id(orderColumn)
                + " DESC LIMIT 1";
    }

    /**
     * Проверка настроек Flink-таблицы.
     */
    private void validateFlinkDb(
            ComparisonProperties.Database db
    ) {
        if (db == null) {
            throw new IllegalStateException(
                    "Flink comparison configuration is missing"
            );
        }

        if (blank(db.getTable())) {
            throw new IllegalStateException(
                    "Flink comparison table is not configured"
            );
        }

        if (blank(db.getEventIdColumn())) {
            throw new IllegalStateException(
                    "Flink event-id-column is not configured"
            );
        }

        if (blank(db.getActionTypeColumn())) {
            throw new IllegalStateException(
                    "Flink action-type-column is not configured"
            );
        }

        if (blank(db.getDataJsonColumn())) {
            throw new IllegalStateException(
                    "Flink data-json-column is not configured"
            );
        }

        validateIdentifiers(db);
    }

    /**
     * Для legacy eventId-column не нужен,
     * потому что eventId лежит внутри data_json.
     */
    private void validateLegacyDb(
            ComparisonProperties.Database db
    ) {
        if (db == null) {
            throw new IllegalStateException(
                    "Legacy comparison configuration is missing"
            );
        }

        if (blank(db.getTable())) {
            throw new IllegalStateException(
                    "Legacy comparison table is not configured"
            );
        }

        if (blank(db.getActionTypeColumn())) {
            throw new IllegalStateException(
                    "Legacy action-type-column is not configured"
            );
        }

        if (blank(db.getDataJsonColumn())) {
            throw new IllegalStateException(
                    "Legacy data-json-column is not configured"
            );
        }

        id(db.getTable());
        id(db.getActionTypeColumn());
        id(db.getDataJsonColumn());

        if (!blank(db.getOrderColumn())) {
            id(db.getOrderColumn());
        }
    }

    private void validateIdentifiers(
            ComparisonProperties.Database db
    ) {
        id(db.getTable());

        if (!blank(db.getEventIdColumn())) {
            id(db.getEventIdColumn());
        }

        id(db.getActionTypeColumn());
        id(db.getDataJsonColumn());

        if (!blank(db.getOrderColumn())) {
            id(db.getOrderColumn());
        }
    }

    /**
     * Названия таблиц/колонок нельзя передавать как JDBC ? parameter,
     * поэтому проверяем их whitelist-regex.
     *
     * Разрешаем:
     *
     * table
     * schema.table
     */
    private String id(
            String value
    ) {
        if (blank(value)
                || !SAFE_IDENTIFIER.matcher(value).matches()) {

            throw new IllegalArgumentException(
                    "Unsafe SQL identifier: " + value
            );
        }

        return value;
    }

    private boolean blank(
            String value
    ) {
        return value == null
                || value.isBlank();
    }
}