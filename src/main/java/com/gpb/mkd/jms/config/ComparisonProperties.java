package com.gpb.mkd.jms.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "app.comparison")
public class ComparisonProperties {

    private int maxDifferences = 1000;
    private int maxValueLength = 1000;

    private Database flink = new Database();
    private Database legacy = new Database();

    @Data
    public static class Database {

        private String table;

        private String eventIdColumn = "event_id";

        private String actionTypeColumn = "action_type";

        private String dataJsonColumn = "data_json";

        private String orderColumn = "id";
    }
}