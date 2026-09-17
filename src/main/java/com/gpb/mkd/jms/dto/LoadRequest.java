package com.gpb.mkd.jms.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

@Getter
@Setter
public class LoadRequest {

    /**
     * Тело сообщения, которое уйдет в MQ.
     */
    @NotNull 
    private JsonNode payload;

    /**
     * Сколько сообщений отправить.
     */
    @Min(1)
    private int count = 1;

    /**
     * Количество параллельных потоков.
     * Если null, используется app.artemis.concurrent-senders.
     */
    @Min(1)
    private Integer concurrency;

    /**
     * Дополнительные JMS properties.
     */
    private Map<String, String> properties;

    /**
     * Нужно ли сохранять ответы в HTTP response.
     * Для большой нагрузки лучше false.
     */
    private boolean includeResponses = false;
}
