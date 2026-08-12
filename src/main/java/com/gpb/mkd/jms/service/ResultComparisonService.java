package com.gpb.mkd.jms.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.mkd.jms.dto.CompareActionType;
import com.gpb.mkd.jms.dto.ComparisonResponse;
import com.gpb.mkd.jms.dto.DifferenceType;
import com.gpb.mkd.jms.dto.JsonDifference;
import com.gpb.mkd.jms.repository.ComparisonRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;

@Slf4j
@Service
@RequiredArgsConstructor
public class ResultComparisonService {

    private static final Set<String> TECHNICAL_ROOT_FIELDS = Set.of(
            "dfw_query_id",
            "dfw_event_id",
            "dfw_action_dttm",
            "dfw_action_type",
            "dfw_created_dttm",
            "dfw_readed_dttm",
            "dfw_readed_from_mq_dttm",
            "dfw_process_metrics",
            "dfw_hostname",
            "dfw_user_login",
            "dfw_load_test_id",
            "dfw_load_test_desc",
            "dfw_request_start_dttm",
            "dfw_request_end_dttm",
            "dfw_request_latency",
            "dfw_process_dttm",
            "dfw_flink_queue_latency",
            "dfw_dotnet_process_start_dttm"
    );

    private final ComparisonRepository repository;
    private final JsonComparisonService jsonComparisonService;

    public ComparisonResponse compare(
            String eventId,
            CompareActionType actionType
    ) {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException(
                    "eventId must not be blank"
            );
        }

        if (actionType == null) {
            throw new IllegalArgumentException(
                    "actionType must be ANSWER or ANSWER_DETAIL"
            );
        }

        String normalizedEventId =
                eventId.trim();

        log.info(
                "[COMPARE][eventId={}][actionType={}] comparison START",
                normalizedEventId,
                actionType
        );

        /* Получаем результат нового Flink
              Ищем event_id = eventId action_type = ANSWER / ANSWER_DETAIL
         */
        JsonNode flinkResult =
                repository.findFlinkResult(
                                normalizedEventId,
                                actionType
                        )
                        .orElseThrow(
                                () -> new IllegalStateException(
                                        "Flink result not found for eventId="
                                                + normalizedEventId
                                                + ", actionType="
                                                + actionType
                                )
                        );

        log.info(
                "[COMPARE][eventId={}][actionType={}] Flink result FOUND",
                normalizedEventId,
                actionType
        );

        /*
          Находим старый QUERY
         * action_type = QUERY
         * и внутри data_json dfw_event_id = eventId
         */
        JsonNode legacyQuery =
                repository.findLegacyQueryByEventId(
                                normalizedEventId
                        )
                        .orElseThrow(
                                () -> new IllegalStateException(
                                        "Legacy QUERY not found by "
                                                + "data_json.dfw_event_id="
                                                + normalizedEventId
                                )
                        );

        log.info(
                "[COMPARE][eventId={}][actionType={}] Legacy QUERY FOUND",
                normalizedEventId,
                actionType
        );

        /*
         *  Из QUERY достаём dfw_query_id
         */
        String rawLegacyQueryId =
                legacyQuery
                        .path("dfw_query_id")
                        .asText(null);

        if (rawLegacyQueryId == null || rawLegacyQueryId.isBlank()) {
            throw new IllegalStateException(
                    "Legacy QUERY found, but "
                            + "data_json.dfw_query_id is empty for eventId="
                            + normalizedEventId
            );
        }

        String legacyQueryId = rawLegacyQueryId.trim();

        log.info(
                "[COMPARE][eventId={}][actionType={}] legacyQueryId={}",
                normalizedEventId,
                actionType,
                legacyQueryId
        );

        JsonNode legacyResult =
                repository.findLegacyResult(
                                legacyQueryId,
                                actionType
                        )
                        .orElseThrow(
                                () -> new IllegalStateException(
                                        "Legacy result not found for dfw_query_id="
                                                + legacyQueryId
                                                + ", actionType="
                                                + actionType
                                )
                        );

        log.info(
                "[COMPARE][eventId={}][actionType={}] Legacy result FOUND queryId={}",
                normalizedEventId,
                actionType,
                legacyQueryId
        );

        /*Подготавливаем JSON для бизнес-сравнения

         */
        JsonNode flinkComparable =
                toComparableResult(
                        flinkResult,
                        actionType,
                        "FLINK"
                );

        JsonNode legacyComparable =
                toComparableResult(
                        legacyResult,
                        actionType,
                        "LEGACY"
                );

        log.info(
                "[COMPARE][eventId={}][actionType={}] comparing "
                        + "flinkNodeType={} legacyNodeType={} "
                        + "flinkSize={} legacySize={}",
                normalizedEventId,
                actionType,
                nodeType(flinkComparable),
                nodeType(legacyComparable),
                nodeSize(flinkComparable),
                nodeSize(legacyComparable)
        );

        //Рекурсивное сравнение JSON
        List<JsonDifference> differences =
                jsonComparisonService.compare(
                        flinkComparable,
                        legacyComparable
                );


         //Считаем статистику различий

        int valueMismatch =
                count(
                        differences,
                        DifferenceType.VALUE_MISMATCH
                );

        int typeMismatch =
                count(
                        differences,
                        DifferenceType.TYPE_MISMATCH
                );

        int onlyInFlink =
                count(
                        differences,
                        DifferenceType.ONLY_IN_FLINK
                );

        int onlyInLegacy =
                count(
                        differences,
                        DifferenceType.ONLY_IN_LEGACY
                );

        boolean match =
                differences.isEmpty();


        //Логируем результат

        if (match) {

            log.info(
                    "[COMPARE][eventId={}][actionType={}] "
                            + "MATCH legacyQueryId={} "
                            + "- business results are identical",
                    normalizedEventId,
                    actionType,
                    legacyQueryId
            );

        } else {

            log.warn(
                    "[COMPARE][eventId={}][actionType={}] "
                            + "DIFFER legacyQueryId={} "
                            + "totalDifferences={} "
                            + "valueMismatch={} "
                            + "typeMismatch={} "
                            + "onlyInFlink={} "
                            + "onlyInLegacy={}",
                    normalizedEventId,
                    actionType,
                    legacyQueryId,
                    differences.size(),
                    valueMismatch,
                    typeMismatch,
                    onlyInFlink,
                    onlyInLegacy
            );

            for (JsonDifference difference : differences) {

                log.warn(
                        "[COMPARE][eventId={}][actionType={}] "
                                + "differenceType={} "
                                + "path={} "
                                + "flink={} "
                                + "legacy={}",
                        normalizedEventId,
                        actionType,
                        difference.type(),
                        difference.path(),
                        difference.flinkValue(),
                        difference.legacyValue()
                );
            }
        }

        log.info(
                "[COMPARE][eventId={}][actionType={}] comparison END match={} totalDifferences={}",
                normalizedEventId,
                actionType,
                match,
                differences.size()
        );

        /*
         Возвращаем результат API
         */
        return new ComparisonResponse(
                normalizedEventId,
                actionType,
                match,
                legacyQueryId,
                valueMismatch,
                onlyInFlink,
                onlyInLegacy,
                typeMismatch,
                differences.size(),
                differences
        );
    }

    /**
     * Формирует JSON, который реально надо сравнивать.
     *
     * ANSWER_DETAIL:
     *
     * сравниваем только:
     *
     * detail_results
     *
     * Таким образом технические поля:
     *
     * dfw_query_id
     * timestamps
     * hostname
     * metrics
     * eventId
     *
     * не влияют на результат.
     *
     *
     * ANSWER:
     *
     * сравниваем весь JSON,
     * но удаляем известные технические поля верхнего уровня.
     */
    private JsonNode toComparableResult(
            JsonNode source,
            CompareActionType actionType,
            String sourceName
    ) {
        if (source == null || source.isNull()) {
            throw new IllegalStateException(
                    sourceName + " result is null"
            );
        }

        /*
         ANSWER_DETAIL
         */
        if (actionType == CompareActionType.ANSWER_DETAIL) {

            JsonNode detailResults =
                    source.get("detail_results");

            if (detailResults == null
                    || detailResults.isNull()
                    || detailResults.isMissingNode()) {

                throw new IllegalStateException(
                        sourceName
                                + " ANSWER_DETAIL does not contain detail_results"
                );
            }

            log.info(
                    "[COMPARE] {} ANSWER_DETAIL -> comparing detail_results only",
                    sourceName
            );

            return detailResults.deepCopy();
        }

        /*
          ANSWER
         */
        JsonNode copy =
                source.deepCopy();

        if (copy instanceof ObjectNode objectNode) {

            TECHNICAL_ROOT_FIELDS.forEach(
                    objectNode::remove
            );
        }

        log.info(
                "[COMPARE] {} ANSWER -> comparing JSON without technical root fields",
                sourceName
        );

        return copy;
    }

    private int count(
            List<JsonDifference> differences,
            DifferenceType type
    ) {
        return (int) differences
                .stream()
                .filter(
                        difference ->
                                difference.type() == type
                )
                .count();
    }

    private String nodeType(
            JsonNode node
    ) {
        if (node == null) {
            return "NULL";
        }

        return node
                .getNodeType()
                .name();
    }

    private int nodeSize(
            JsonNode node
    ) {
        if (node == null) {
            return 0;
        }

        if (node.isObject()
                || node.isArray()) {

            return node.size();
        }

        return 1;
    }
}