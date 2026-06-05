# JMS Publisher

`jms-publisher` — Spring Boot микросервис для нагрузочного тестирования request-response обмена через Apache ActiveMQ Artemis по JMS.

Сервис принимает HTTP `POST` запрос, отправляет заданное количество сообщений в request queue, ожидает ответы из shared reply queue и собирает метрики по скорости, задержкам, ошибкам и таймаутам.

## Назначение

Проект нужен для проверки сценария:

```text
HTTP POST
  -> jms-publisher
  -> Artemis JMS request queue
  -> provider / обработчик
  -> Artemis JMS reply queue
  -> jms-publisher reply listener
  -> HTTP response + metrics
```

Основной сценарий совместимости:

```text
JMS producer -> Artemis / bridge / adapter -> provider, который читает IBM MQ MQMD
```

При этом сервис формирует JMS-заголовки так, чтобы после преобразования/моста в IBM MQ provider мог работать с MQMD-полями.

## Архитектура обмена

Сервис использует оптимизированную модель для нагрузки:

1. Один long-lived JMS `Connection` на весь сервис.
2. Один общий listener на `replyQueue`.
3. Пул sender-потоков.
4. У каждого sender-потока свой `Session` и `MessageProducer` через `ThreadLocal`.
5. Ответы сопоставляются через `ConcurrentHashMap<correlationId, CompletableFuture<...>>`.
6. Для защиты от очень быстрых ответов используется `earlyReplies` buffer.

Упрощенно:

```text
LoadTestController
  -> JmsRequestReplyService
      -> senderExecutor
          -> ThreadLocal(Session + Producer)
          -> requestQueue
      <- replyConsumer MessageListener
          <- replyQueue
          -> pendingRequests[correlationId].complete(...)
```

## Request-response correlation

По умолчанию используется рекомендуемый паттерн для provider, который работает через IBM MQ MQMD:

```text
Request:
  JMSMessageID  -> после bridge в IBM MQ обычно соответствует MQMD.MsgId
  JMSReplyTo    -> после bridge в IBM MQ обычно соответствует MQMD.ReplyToQ

Response:
  JMSCorrelationID -> должен быть равен request JMSMessageID
  MQMD.CorrelId    -> должен быть равен request MQMD.MsgId
```

То есть provider должен вернуть ответ так:

```text
response destination = request.JMSReplyTo / MQMD.ReplyToQ
response correlation = request.JMSMessageID / MQMD.MsgId
```

В IBM MQ native-логике это обычно выглядит так:

```text
response.MQMD.CorrelId = request.MQMD.MsgId
response.MQMD.MsgType  = MQMT_REPLY
response.MQMD.Format   = MQSTR, если тело строковое
```

## Важный нюанс про MQMD

MQMD — это нативный заголовок IBM MQ. В Apache Artemis самого `MQMD` нет.

Этот сервис отправляет JMS-сообщения в Artemis. MQMD-поля появятся только если дальше есть bridge/adaptor/IBM MQ JMS provider, который преобразует JMS-заголовки/properties в IBM MQ message descriptor.

Минимально важные JMS-заголовки:

| JMS | IBM MQ / MQMD смысл |
|---|---|
| `JMSMessageID` | обычно соответствует `MQMD.MsgId` |
| `JMSReplyTo` | обычно соответствует `MQMD.ReplyToQ` / `ReplyToQMgr` |
| `JMSCorrelationID` ответа | обычно соответствует `MQMD.CorrelId` ответа |

По умолчанию request `JMSCorrelationID` не задается. Это сделано специально, чтобы provider возвращал:

```text
response.CorrelId = request.MsgId
```

## Конфигурация

Файл конфигурации:

```text
src/main/resources/application.yaml
```

Основной блок:

```yaml
server:
  port: 9000

spring:
  application:
    name: jms-publisher

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  metrics:
    tags:
      application: ${spring.application.name}

app:
  artemis:
    broker-url: "ssl://artemis-host:61617?trustStorePath=/opt/app/certs/truststore.p12;trustStorePassword=${TRUSTSTORE_PASSWORD};trustStoreType=PKCS12"

    username: ${ARTEMIS_USERNAME}
    password: ${ARTEMIS_PASSWORD}

    request-queue: REQUEST.Q
    reply-queue: REPLY.Q

    receive-timeout-ms: 30000
    concurrent-senders: 20

    delivery-mode-persistent: true
    message-ttl-ms: 60000

    early-reply-ttl-ms: 60000
    early-reply-max-size: 10000
```

### Без TLS

```yaml
app:
  artemis:
    broker-url: "tcp://localhost:61616"
    username: admin
    password: admin
```

### TLS, только server authentication

```yaml
app:
  artemis:
    broker-url: "ssl://artemis-host:61617?trustStorePath=/opt/app/certs/truststore.p12;trustStorePassword=${TRUSTSTORE_PASSWORD};trustStoreType=PKCS12"
```

### Mutual TLS

```yaml
app:
  artemis:
    broker-url: "ssl://artemis-host:61617?trustStorePath=/opt/app/certs/truststore.p12;trustStorePassword=${TRUSTSTORE_PASSWORD};trustStoreType=PKCS12;keyStorePath=/opt/app/certs/keystore.p12;keyStorePassword=${KEYSTORE_PASSWORD};keyStoreType=PKCS12"
```

Переменные окружения для TLS/auth:

```bash
export ARTEMIS_USERNAME=admin
export ARTEMIS_PASSWORD=admin
export TRUSTSTORE_PASSWORD=changeit
export KEYSTORE_PASSWORD=changeit
```

## IBM MQ compatibility properties

В `application.yaml` есть блок:

```yaml
app:
  artemis:
    ibm-mq-compatibility:
      reply-to-enabled: true

      request-correlation-id-enabled: false
      request-correlation-id-prefix: "REQ-"

      ibm-properties-enabled: false
      mq-message-type: 1
      mq-format: MQSTR
      character-set: 1208
      encoding: 546
```

### `reply-to-enabled`

Должно быть `true` для request-response.

Сервис устанавливает:

```text
message.setJMSReplyTo(replyQueue)
```

Это нужно, чтобы provider понимал, куда вернуть ответ.

### `request-correlation-id-enabled`

По умолчанию `false`.

Рекомендуемый вариант:

```text
request JMSCorrelationID не задаем
response JMSCorrelationID = request JMSMessageID
```

Если поставить `true`, сервис будет генерировать request `JMSCorrelationID` вида:

```text
REQ-<uuid>
```

Тогда provider должен вернуть ответ уже по другой схеме:

```text
response JMSCorrelationID = request JMSCorrelationID
```

Для IBM MQ native/MQMD provider обычно лучше оставить `false`.

### `ibm-properties-enabled`

По умолчанию `false`.

Если включить, сервис добавит properties:

```text
JMS_IBM_MsgType
JMS_IBM_Format
JMS_IBM_Character_Set
JMS_IBM_Encoding
```

Они могут быть полезны, если сообщение идет через IBM MQ JMS provider или bridge, который учитывает эти свойства.

Для pure Artemis эти значения будут обычными JMS properties и сами по себе не создадут MQMD.

## HTTP API

### Запуск request-response нагрузки

```http
POST /api/load/request-reply
Content-Type: application/json
```

Тело запроса:

```json
{
  "payload": "{\"clientId\":\"123\",\"operation\":\"CHECK\"}",
  "count": 100,
  "concurrency": 10,
  "includeResponses": false,
  "properties": {
    "EventClass": "LOAD_TEST",
    "SourceSystem": "JMS_PUBLISHER"
  }
}
```

Поля запроса:

| Поле | Тип | Описание |
|---|---|---|
| `payload` | string | Тело JMS `TextMessage` |
| `count` | int | Количество сообщений |
| `concurrency` | int/null | Параллелизм внутри запуска. Не может быть больше `app.artemis.concurrent-senders` |
| `includeResponses` | boolean | Возвращать ли тела ответов в HTTP response |
| `properties` | object | Дополнительные JMS string properties |

Пример `curl`:

```bash
curl -X POST http://localhost:9000/api/load/request-reply \
  -H "Content-Type: application/json" \
  -d '{
    "payload": "{\"clientId\":\"123\",\"operation\":\"CHECK\"}",
    "count": 1000,
    "concurrency": 20,
    "includeResponses": false,
    "properties": {
      "EventClass": "LOAD_TEST",
      "SourceSystem": "JMS_PUBLISHER"
    }
  }'
```

Пример ответа:

```json
{
  "requested": 1000,
  "success": 998,
  "failed": 2,
  "totalDurationMs": 12500,
  "requestsPerSecond": 79.84,
  "avgLatencyMs": 210.5,
  "minLatencyMs": 25,
  "maxLatencyMs": 3010,
  "results": []
}
```

Если `includeResponses=true`, в `results` будут результаты по каждому сообщению:

```json
{
  "index": 1,
  "success": true,
  "requestMessageId": "ID:...",
  "responseCorrelationId": "ID:...",
  "responseBody": "...",
  "durationMs": 123,
  "error": null
}
```

Для большой нагрузки лучше оставлять:

```json
"includeResponses": false
```

Иначе HTTP response может стать очень большим и сам станет узким местом теста.

## Метрики

Actuator endpoints:

```text
GET /actuator/health
GET /actuator/metrics
GET /actuator/prometheus
```

Кастомные метрики:

| Метрика | Описание |
|---|---|
| `mq.load.sent.total` | всего отправлено request-сообщений |
| `mq.load.success.total` | успешные request-response операции |
| `mq.load.failed.total` | все ошибки, включая timeout/send error |
| `mq.load.timeout.total` | количество таймаутов ожидания ответа |
| `mq.load.send.failed.total` | ошибки отправки |
| `mq.load.reply.received.total` | получено reply-сообщений |
| `mq.load.reply.without_pending.total` | reply без найденного pending request на момент получения |
| `mq.load.reply.early.matched.total` | ранние reply, успешно сматченные позже |
| `mq.load.inflight` | текущие активные отправки |
| `mq.load.pending` | количество ожидаемых ответов |
| `mq.load.early_replies` | размер буфера ранних ответов |
| `mq.load.latency` | latency request-response |
| `mq.load.payload.size.bytes` | размер request payload |
| `mq.load.response.size.bytes` | размер response payload |

Prometheus:

```bash
curl http://localhost:9000/actuator/prometheus
```

Примеры метрик в Prometheus-формате:

```text
mq_load_sent_total 1000.0
mq_load_success_total 998.0
mq_load_failed_total 2.0
mq_load_timeout_total 2.0
mq_load_inflight 0.0
mq_load_pending 0.0
mq_load_latency_seconds_count 998.0
mq_load_latency_seconds_sum 210.45
mq_load_latency_seconds_max 3.01
```

## Сборка и запуск

### Сборка

```bash
./mvnw clean package
```

### Запуск jar

```bash
java -jar target/jms-publisher-*.jar
```

С переменными окружения:

```bash
ARTEMIS_USERNAME=admin \
ARTEMIS_PASSWORD=admin \
TRUSTSTORE_PASSWORD=changeit \
java -jar target/jms-publisher-*.jar
```

### Переопределение параметров через environment variables

Spring Boot позволяет переопределять YAML-параметры через env:

```bash
export APP_ARTEMIS_BROKER_URL='tcp://localhost:61616'
export APP_ARTEMIS_USERNAME='admin'
export APP_ARTEMIS_PASSWORD='admin'
export APP_ARTEMIS_REQUEST_QUEUE='REQUEST.Q'
export APP_ARTEMIS_REPLY_QUEUE='REPLY.Q'
export APP_ARTEMIS_CONCURRENT_SENDERS='20'
export APP_ARTEMIS_RECEIVE_TIMEOUT_MS='30000'
```

После этого:

```bash
./mvnw spring-boot:run
```

## Требования к provider

Чтобы сервис корректно получил ответ, provider обязан:

1. Прочитать request из `requestQueue`.
2. Получить queue для ответа из `JMSReplyTo` / `MQMD.ReplyToQ`.
3. Отправить response в эту reply queue.
4. Установить correlation id ответа.

Для дефолтного режима:

```text
response.JMSCorrelationID = request.JMSMessageID
```

Для IBM MQ MQMD provider:

```text
response.MQMD.CorrelId = request.MQMD.MsgId
```

Если включен `request-correlation-id-enabled=true`, тогда:

```text
response.JMSCorrelationID = request.JMSCorrelationID
response.MQMD.CorrelId    = request.MQMD.CorrelId
```

## Рекомендации для нагрузочного тестирования

1. На больших объемах держать `includeResponses=false`.
2. `count` делать существенно больше `concurrency`, например:

```json
{
  "count": 100000,
  "concurrency": 50
}
```

3. `concurrency` в запросе не может превысить `app.artemis.concurrent-senders`.
4. Если нужны 50 потоков отправки, выставить:

```yaml
app:
  artemis:
    concurrent-senders: 50
```

5. Для latency смотреть не только HTTP response, но и `/actuator/prometheus`:

```text
mq_load_latency_seconds_count
mq_load_latency_seconds_sum
mq_load_latency_seconds_max
```

6. Если растет `mq.load.pending`, provider не успевает отвечать или ответы не матчятся по correlation id.
7. Если растет `mq.load.timeout.total`, проверить:
   - правильно ли provider ставит `JMSCorrelationID` / `MQMD.CorrelId`;
   - отправляет ли provider ответ в `JMSReplyTo` / `ReplyToQ`;
   - хватает ли `receive-timeout-ms`;
   - не очищается ли reply queue другим consumer'ом.
8. Если растет `mq.load.reply.without_pending.total`, возможны:
   - в reply queue лежат старые ответы;
   - несколько тестов используют одну reply queue;
   - provider возвращает неверный correlation id;
   - ответ приходит после timeout.

## Типовые проблемы

### Ответы уходят в reply queue, но сервис получает timeout

Проверить correlation:

```text
response.JMSCorrelationID == request.JMSMessageID
```

Для MQMD:

```text
response.MQMD.CorrelId == request.MQMD.MsgId
```

### Provider не видит ReplyToQ

Проверить:

```yaml
app:
  artemis:
    ibm-mq-compatibility:
      reply-to-enabled: true
```

И убедиться, что bridge/adaptor переносит `JMSReplyTo` в IBM MQ `MQMD.ReplyToQ`.

### TLS handshake error

Проверить:

1. правильный `ssl://host:port`;
2. truststore содержит CA/сертификат брокера;
3. пароль truststore корректный;
4. `trustStoreType=PKCS12`, если используется `.p12`;
5. при mutual TLS указан `keyStorePath` и `keyStorePassword`.

### Низкая скорость

Проверить:

1. `app.artemis.concurrent-senders`;
2. `concurrency` в HTTP request;
3. persistent/non-persistent delivery mode;
4. размер payload;
5. скорость provider;
6. broker disk sync, если `delivery-mode-persistent=true`.

Для максимальной скорости в тестовой среде можно проверить режим:

```yaml
app:
  artemis:
    delivery-mode-persistent: false
```

## Текущие ограничения

1. Payload отправляется как `TextMessage`.
2. Reply body читается как `TextMessage` или `message.getBody(String.class)`.
3. Один общий `replyQueue` используется для всех запросов.
4. При одновременном запуске нескольких экземпляров сервиса с одной reply queue возможны чужие reply-сообщения. Для изоляции лучше использовать разные reply queue на экземпляр или отдельный идентификатор теста в properties.
5. `JMS_IBM_*` properties не гарантируют создание MQMD в Artemis. Они имеют смысл только при соответствующем bridge/adaptor/IBM MQ JMS provider.

## Быстрая проверка

1. Запустить Artemis.
2. Указать очереди:

```yaml
app:
  artemis:
    broker-url: "tcp://localhost:61616"
    username: admin
    password: admin
    request-queue: REQUEST.Q
    reply-queue: REPLY.Q
```

3. Запустить сервис:

```bash
./mvnw spring-boot:run
```

4. Отправить тест:

```bash
curl -X POST http://localhost:9000/api/load/request-reply \
  -H "Content-Type: application/json" \
  -d '{
    "payload": "hello",
    "count": 1,
    "concurrency": 1,
    "includeResponses": true
  }'
```

5. Проверить метрики:

```bash
curl http://localhost:9000/actuator/metrics
curl http://localhost:9000/actuator/prometheus
```

---

## Обновленная конфигурация подключения к Artemis HA/TLS

Подключение можно задать двумя способами.

### 1. Рекомендуемый способ: список хостов

Если `app.artemis.broker-url` не указан, сервис сам собирает multi-host broker URL из `hosts` и параметров подключения:

```yaml
app:
  artemis:
    scheme: ssl
    hosts:
      - host: artemis-1.example.local
        port: 61617
      - host: artemis-2.example.local
        port: 61617

    client-id: ${ARTEMIS_CLIENT_ID:jms-publisher-1}

    ha: true
    initial-connect-attempts: 3
    reconnect-attempts: -1
    retry-interval-ms: 2500
    block-on-non-durable-send: true
    consumer-window-size: 0

    ssl-enabled: true
    enabled-cipher-suites: ${ARTEMIS_ENABLED_CIPHER_SUITES:TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384}

    trust-store-path: /opt/app/certs/truststore.p12
    trust-store-password: ${TRUSTSTORE_PASSWORD}
    trust-store-type: PKCS12

    # Если нужен mutual TLS:
    # key-store-path: /opt/app/certs/keystore.p12
    # key-store-password: ${KEYSTORE_PASSWORD}
    # key-store-type: PKCS12
```

Из этого будет собран URL вида:

```text
(ssl://artemis-1.example.local:61617,ssl://artemis-2.example.local:61617)?ha=true;clientID=jms-publisher-1;initialConnectAttempts=3;reconnectAttempts=-1;retryInterval=2500;blockOnNonDurableSend=true;consumerWindowSize=0;sslEnabled=true;enabledCipherSuites=...
```

### 2. Полный broker-url вручную

Если нужно полностью контролировать строку подключения, можно указать `broker-url`. В этом случае `hosts`, `scheme` и параметры сборки URL не используются:

```yaml
app:
  artemis:
    broker-url: "(ssl://artemis-1.example.local:61617,ssl://artemis-2.example.local:61617)?ha=true;clientID=jms-publisher-1;initialConnectAttempts=3;reconnectAttempts=-1;retryInterval=2500;blockOnNonDurableSend=true;consumerWindowSize=0;sslEnabled=true;enabledCipherSuites=TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384;trustStorePath=/opt/app/certs/truststore.p12;trustStorePassword=${TRUSTSTORE_PASSWORD};trustStoreType=PKCS12"
```

### Важные параметры

| Параметр | Значение по умолчанию | Назначение |
|---|---:|---|
| `ha` | `true` | HA/failover режим клиента |
| `client-id` | из `application.yaml` | JMS ClientID. Должен быть уникальным для одновременно работающих инстансов |
| `initial-connect-attempts` | `3` | Количество попыток первичного подключения |
| `reconnect-attempts` | `-1` | Бесконечные reconnection attempts |
| `retry-interval-ms` | `2500` | Интервал между попытками reconnect |
| `block-on-non-durable-send` | `true` | Блокировать отправку non-durable сообщений до подтверждения брокера |
| `consumer-window-size` | `0` | Отключить prefetch/window у consumer; полезно для строгого request-response теста |
| `ssl-enabled` | `true` | Включение TLS-параметров в URL |
| `enabled-cipher-suites` | из `application.yaml` | Разрешенные TLS cipher suites |

`JmsConfig` дополнительно выставляет эти параметры на `ActiveMQConnectionFactory` через setters: `clientID`, `initialConnectAttempts`, `reconnectAttempts`, `retryInterval`, `blockOnNonDurableSend`, `consumerWindowSize`, `callTimeout`, `callFailoverTimeout`.
