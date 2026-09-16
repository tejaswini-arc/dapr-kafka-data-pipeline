# Dapr + Kafka + Spring Boot + MySQL POC
## Final POC Documentation

**Project:** `dapr-kafka-data-pipeline`  
**Environment:** Windows + Docker Compose  

---

# What is Dapr?
Dapr (Distributed Application Runtime) is an open-source runtime that helps developers build distributed and microservices-based applications by providing common infrastructure capabilities through APIs and sidecars.
In this POC, Dapr does not replace Kafka's durability. Kafka is the durable message broker; Dapr provides the messaging abstraction, delivery, retries, and integration.

# Dapr provides capabilities such as
1. Pub/Sub
2. Service-to-service invocation
3. State management
4. Secrets
5. Distributed locks
6. Bindings
7. Configuration
8. Workflows
9. Observability


# 1. POC Objective

The objective of this POC was to build and validate an event-driven data pipeline using:

- Java 17
- Spring Boot 4.1.1
- Dapr 1.17.14
- Apache Kafka
- MySQL 8.0
- Docker / Docker Compose
- Offset Explorer (Analyze)


The POC demonstrates how a Spring Boot application can publish an order through Dapr Pub/Sub, have Dapr deliver the event through Kafka, consume the event through a Dapr subscription, and finally persist the consumed order into MySQL.

---

# 2. Final Architecture

```text
                         ┌──────────────────────┐
                         │      Client / UI      │
                         │  POST /orders         │
                         └──────────┬───────────┘
                                    │
                                    ▼
                    ┌─────────────────────────────┐
                    │       Spring Boot            │
                    │        order-app             │
                    │                              │
                    │  OrderService               │
                    │  - Receives Order           │
                    │  - Publishes through Dapr   │
                    └──────────────┬──────────────┘
                                   │
                                   │ HTTP
                                   ▼
                    ┌─────────────────────────────┐
                    │       Dapr Sidecar           │
                    │      order-app-dapr          │
                    │                              │
                    │ Dapr HTTP :3500             │
                    │ Pub/Sub abstraction          │
                    └──────────────┬──────────────┘
                                   │
                                   │ Kafka protocol
                                   ▼
                    ┌─────────────────────────────┐
                    │           Kafka              │
                    │                              │
                    │ Topic: orders                │
                    │ Partitions: 0, 1, 2          │
                    │                              │
                    │ Consumer Group:              │
                    │ order-app-group              │
                    └──────────────┬──────────────┘
                                   │
                                   │ Consume
                                   ▼
                    ┌─────────────────────────────┐
                    │       Dapr Sidecar           │
                    │      order-app-dapr          │
                    │                              │
                    │ Subscription: orders         │
                    │ Route: POST /consume         │
                    └──────────────┬──────────────┘
                                   │
                                   │ HTTP
                                   ▼
                    ┌─────────────────────────────┐
                    │       Spring Boot            │
                    │        /consume              │
                    │                              │
                    │  1. Receive CloudEvent       │
                    │  2. Extract data             │
                    │  3. Convert to Order         │
                    │  4. Save to MySQL            │
                    └──────────────┬──────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────┐
                    │           MySQL              │
                    │          ordersdb             │
                    │                              │
                    │ Consumed order records       │
                    └─────────────────────────────┘
```

---

# 3. Docker Architecture

The final Docker environment contains:

```text
Docker Host
│
├── Zookeeper
│   └── Port 2181
│
├── Kafka
│   ├── Internal: kafka:29092
│   └── External: localhost:9092
│
├── MySQL
│   └── Database: ordersdb
│
├── order-app
│   └── Spring Boot :8080
│
└── order-app-dapr
    ├── Dapr HTTP :3500
    └── Dapr gRPC :50001
```

All application containers are connected to:

```text
dapr-kafka-data-pipeline_app-network
```

---

# 4. Why Kafka Has Two Listeners

Kafka uses two listeners because there are two different networking environments.

```text
Docker containers
       │
       └── kafka:29092

Windows host applications
       │
       └── localhost:9092
```

Kafka configuration:

```yaml
KAFKA_LISTENERS: INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092

KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092

KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT

KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
```

Therefore:

- Dapr inside Docker uses `kafka:29092`.
- Offset Explorer running on Windows uses `localhost:9092`.

---

# 5. Docker Compose Services

The final setup contains these services:

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    ports:
      - "9092:9092"

  mysql:
    image: mysql:8.0

  order-app:
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "8080:8080"

  order-app-dapr:
    image: daprio/daprd:latest
    ports:
      - "3500:3500"
      - "50001:50001"
```

MySQL uses a persistent Docker volume:

```yaml
volumes:
  - mysql-data:/var/lib/mysql
```

This keeps MySQL data persistent across normal container recreation.

---

# 6. Kafka Topic

The Kafka topic was manually created:

```powershell
docker exec kafka kafka-topics --create `
  --topic orders `
  --bootstrap-server kafka:29092 `
  --partitions 3 `
  --replication-factor 1
```

Verification:

```powershell
docker exec kafka kafka-topics --list `
  --bootstrap-server kafka:29092
```

Result:

```text
orders
```

The topic contains:

```text
orders
├── Partition 0
├── Partition 1
└── Partition 2
```

---

# 7. Dapr Component

The Dapr Pub/Sub component is:

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component

metadata:
  name: pubsub

spec:
  type: pubsub.kafka
  version: v1

  metadata:
    - name: brokers
      value: kafka:29092

    - name: consumerGroup
      value: order-app-group

    - name: authType
      value: none
```

Important:

```text
Component name = pubsub
Kafka broker = kafka:29092
Consumer group = order-app-group
```

---

# 8. Dapr Subscription

The subscription connects Kafka topic `orders` to the Spring Boot `/consume` endpoint.

```yaml
apiVersion: dapr.io/v2alpha1
kind: Subscription

metadata:
  name: orders-subscription

spec:
  pubsubname: pubsub
  topic: orders

  routes:
    default: /consume

scopes:
  - order-app
```

This means:

```text
Kafka topic orders
       ↓
Dapr Pub/Sub component
       ↓
orders-subscription
       ↓
POST /consume
       ↓
Spring Boot order-app
```

---

# 9. Dapr Sidecar Configuration

The Dapr sidecar is started with:

```yaml
command:
  [
    "./daprd",
    "--app-id",
    "order-app",
    "--app-port",
    "8080",
    "--app-channel-address",
    "order-app",
    "--app-protocol",
    "http",
    "--dapr-http-port",
    "3500",
    "--dapr-grpc-port",
    "50001",
    "--resources-path",
    "/components"
  ]
```

The important settings are:

```text
App ID              = order-app
App port            = 8080
App channel address = order-app
Dapr HTTP port      = 3500
Dapr gRPC port      = 50001
Resources path      = /components
```

The `--app-channel-address order-app` setting allows the sidecar container to reach the Spring Boot application using the Docker service name.

---

# 10. Producer Workflow

The producer endpoint receives an order.

Example request:

```json
{
  "id": 10,
  "customer_name": "Teju",
  "product": "AC",
  "amount": 20000,
  "status": "NEW"
}
```

The Spring Boot producer calls:

```text
http://order-app-dapr:3500/v1.0/publish/pubsub/orders
```

Producer code:

```java
restClient.post()
    .uri("/v1.0/publish/pubsub/orders")
    .body(order)
    .retrieve()
    .toBodilessEntity();
```

The producer does not directly communicate with Kafka.

Instead:

```text
Spring Boot
     ↓
Dapr
     ↓
Kafka
```

---

# 11. Producer-to-Kafka Workflow

```text
POST /orders
     │
     ▼
OrderController
     │
     ▼
OrderService
     │
     ▼
Dapr HTTP API
     │
     │ POST /v1.0/publish/pubsub/orders
     ▼
Dapr Pub/Sub
     │
     ▼
Kafka topic: orders
```

Producer console output:

```text
========== PRODUCER ==========
Publishing order to Dapr...
Order ID      : 10
Customer Name : Teju
Product       : AC
Amount        : 20000
Status        : NEW
[PRODUCER] Order published successfully
==============================
```

---

# 12. Kafka Message Format

Dapr publishes the event using a CloudEvent envelope.

A message is approximately:

```json
{
  "data": {
    "amount": 20000,
    "customer_name": "Teju",
    "id": 10,
    "product": "AC",
    "status": "NEW"
  },
  "datacontenttype": "application/json",
  "id": "cd504acc-ef75-4e95-b69a-11dd9568aa10",
  "pubsubname": "pubsub",
  "source": "order-app",
  "specversion": "1.0",
  "time": "2026-09-16T12:31:26Z",
  "topic": "orders",
  "type": "com.dapr.event.sent"
}
```

The actual business object is inside:

```text
data
```

This became important during debugging.

---

# 13. Consumer Workflow

Dapr consumes the Kafka event using:

```text
Consumer Group: order-app-group
```

The Dapr subscription routes the event to:

```text
POST /consume
```

The workflow is:

```text
Kafka
  │
  ▼
Dapr Pub/Sub
  │
  ▼
orders-subscription
  │
  ▼
POST /consume
  │
  ▼
OrderConsumerController
  │
  ▼
Extract CloudEvent.data
  │
  ▼
Convert data → Order
  │
  ▼
ConsumedOrderRepository
  │
  ▼
MySQL
```

---

# 14. Consumer Controller

The consumer receives the CloudEvent instead of directly receiving `Order`.

```java
@PostMapping("/consume")
public ResponseEntity<?> consumeOrder(
        @RequestBody JsonNode cloudEvent) throws Exception {

    System.out.println();
    System.out.println("========== CONSUMER ==========");
    System.out.println("[CONSUMER] CloudEvent received from Dapr");

    JsonNode data = cloudEvent.get("data");

    Order order = objectMapper.treeToValue(data, Order.class);

    System.out.println("Order ID      : " + order.getId());
    System.out.println("Customer Name : " + order.getCustomer_name());
    System.out.println("Product       : " + order.getProduct());
    System.out.println("Amount        : " + order.getAmount());
    System.out.println("Status        : " + order.getStatus());

    orderRepository.save(order);

    System.out.println("[CONSUMER] Order saved into MySQL");
    System.out.println("==============================");

    return ResponseEntity.ok(
        Map.of(
            "message", "Order consumed and saved successfully",
            "orderId", order.getId()
        )
    );
}
```

The important line is:

```java
JsonNode data = cloudEvent.get("data");
```

Then:

```java
Order order = objectMapper.treeToValue(data, Order.class);
```

---

# 15. Jackson 3 Consideration

The application uses Spring Boot 4 / Spring Framework 7 and therefore the runtime showed Jackson 3:

```text
jackson-databind-3.1.5
```

The correct imports are:

```java
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
```

Instead of:

```java
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
```

The `com.fasterxml.jackson.*` import caused a mismatch with the Jackson version used by the application.

---

# 16. Order Model

The order model is:

```java
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {

    private Long id;

    @JsonProperty("customer_name")
    private String customer_name;

    private String product;

    private BigDecimal amount;

    private String status;
}
```

Example business data:

```json
{
  "id": 10,
  "customer_name": "Teju",
  "product": "AC",
  "amount": 20000,
  "status": "NEW"
}
```

---

# 17. MySQL Persistence Workflow

After Dapr successfully invokes `/consume`:

```text
CloudEvent
    ↓
cloudEvent.data
    ↓
Order object
    ↓
ConsumedOrderRepository
    ↓
MySQL ordersdb
```

The order should then be visible in the corresponding MySQL table.

The MySQL database is:

```text
ordersdb
```

---

# 18. Checking Kafka with Offset Explorer

Offset Explorer 3.0.3 was successfully connected to the Docker Kafka instance.

Connection:

```text
Host: localhost
Port: 9092
Security: PLAINTEXT
```

The reason is:

```text
Offset Explorer
     ↓
Windows Host
     ↓
localhost:9092
     ↓
Kafka Docker Container
```

Inside Offset Explorer:

```text
Docker Kafka-orders
  ├── Brokers
  ├── Topics
  │    ├── orders
  │    │    ├── Partition 0
  │    │    ├── Partition 1
  │    │    └── Partition 2
  │    └── __consumer_offsets
  │
  └── Consumers
       └── order-app-group
```

---

# 19. Why Offset Explorer Shows Hexadecimal Data

Offset Explorer displayed values such as:

```text
7B2264617461223A7B22616D6F756E74223A3230...
```

This is not an error.

Kafka stores message values as bytes.

For example:

```text
7B = {
22 = "
64 = d
61 = a
74 = t
61 = a
22 = "
3A = :
```

Therefore:

```text
7B2264617461223A
```

represents:

```text
{"data":
```

The Kafka message itself is still JSON data; Offset Explorer is displaying the underlying bytes in hexadecimal format.

---

# 20. Checking MySQL with DBeaver

DBeaver can be used as the GUI for the Docker MySQL database.

For Windows-to-Docker access, MySQL should expose:

```yaml
ports:
  - "3306:3306"
```

DBeaver connection:

```text
Database: MySQL

Host:     localhost
Port:     3306
Database: ordersdb
Username: root
Password: root
```

After connecting:

```text
Docker MySQL
   └── ordersdb
       └── Tables
           └── <consumed order table>
```

Open the table using:

```text
Right-click table
       ↓
View Data
       ↓
All Rows
```

Expected business data is similar to:

```text
id | customer_name | product | amount | status
---+---------------+---------+--------+-------
10 | Teju          | AC      | 20000  | NEW
```

---

# 21. End-to-End Workflow

The complete successful workflow is:

```text
                 ORDER EVENT FLOW
                 ================

Client
  │
  │ POST /orders
  ▼
Spring Boot order-app
  │
  │ OrderService
  ▼
Dapr Sidecar
  │
  │ Pub/Sub API
  ▼
Kafka
  │
  │ topic = orders
  │
  │ partition 0/1/2
  ▼
Consumer Group
order-app-group
  │
  ▼
Dapr Subscription
orders-subscription
  │
  │ POST /consume
  ▼
Spring Boot order-app
  │
  │ CloudEvent
  │    └── data
  │          └── Order
  ▼
ConsumedOrderRepository
  │
  ▼
MySQL
ordersdb
```

---

# 22. Complete Technology Interaction

```text
┌──────────────────────────────────────────────────────────┐
│                    Docker Host                           │
│                                                          │
│  ┌──────────────┐                                        │
│  │ order-app    │                                        │
│  │ Spring Boot  │                                        │
│  │ :8080        │                                        │
│  └──────┬───────┘                                        │
│         │                                                │
│         │ HTTP                                           │
│         ▼                                                │
│  ┌──────────────┐       Kafka       ┌─────────────────┐ │
│  │ order-app-   │ ────────────────► │     Kafka       │ │
│  │ dapr :3500   │                   │ orders topic    │ │
│  └──────┬───────┘ ◄──────────────── │ P0 P1 P2        │ │
│         │        consume            └─────────────────┘ │
│         │                                                │
│         │ POST /consume                                  │
│         ▼                                                │
│  ┌──────────────┐                                        │
│  │ order-app    │                                        │
│  │ Consumer     │                                        │
│  └──────┬───────┘                                        │
│         │                                                │
│         ▼                                                │
│  ┌──────────────┐                                        │
│  │    MySQL     │                                        │
│  │  ordersdb    │                                        │
│  └──────────────┘                                        │
│                                                          │
└──────────────────────────────────────────────────────────┘

Windows tools:

Offset Explorer ───────► localhost:9092 ─────► Kafka

DBeaver ───────────────► localhost:3306 ─────► MySQL
```

---

# 23. Important Validation Commands

## Check containers

```powershell
docker ps
```

## Check Compose services

```powershell
docker compose ps
```

## Check Kafka topics

```powershell
docker exec kafka kafka-topics --list --bootstrap-server kafka:29092
```

## Check orders topic

```powershell
docker exec kafka kafka-topics --describe `
  --topic orders `
  --bootstrap-server kafka:29092
```

## Check Dapr logs

```powershell
docker logs order-app-dapr --tail 100
```

## Check application logs

```powershell
docker logs order-app --tail 100
```

## Follow application logs

```powershell
docker logs -f order-app
```

## Test Docker networking

```powershell
docker run --rm `
  --network dapr-kafka-data-pipeline_app-network `
  curlimages/curl:latest `
  http://order-app:8080/
```

A 404 response from `/` is expected because there is no controller mapped to `/`. The important point is that the Spring Boot container is reachable.

## Check Dapr health

```powershell
Invoke-RestMethod http://localhost:3500/v1.0/healthz
```

---

# 24. Issues Faced and Solutions

## Issue 1 — `dapr` command not recognized

### Error

```text
dapr : The term 'dapr' is not recognized as the name of a cmdlet
```

### Cause

The Dapr CLI executable was not available in the Windows PATH.

### Solution

Configure the Dapr installation directory in the Windows Environment Variables PATH.

After updating PATH, open a new PowerShell window and verify:

```powershell
dapr --version
```

---

## Issue 2 — Dapr Pub/Sub component not found

### Error

```text
ERR_PUBSUB_NOT_FOUND
```

### Cause

The Dapr sidecar was not loading the component directory correctly.

### Solution

The sidecar resource path was changed to:

```text
--resources-path /components
```

and the Docker volume was:

```yaml
volumes:
  - ./dapr/components:/components
```

This allowed Dapr to discover:

```text
pubsub
orders-subscription
```

---

## Issue 3 — Duplicate Dapr component

### Error

```text
failed to load components:
duplicate definition of Component name pubsub
```

### Cause

Two component files defined the same:

```yaml
metadata:
  name: pubsub
```

### Solution

Only one `pubsub` component definition was retained in the mounted Dapr components directory.

After correction, Dapr logged:

```text
Component loaded: pubsub (pubsub.kafka/v1)
Found Subscription: orders-subscription
```

---

## Issue 4 — Dapr sidecar waiting for application on port 8080

### Symptom

Dapr logged:

```text
application protocol: http.
waiting on port 8080
waiting for application to listen on port 8080
```

### Cause

The Dapr sidecar and Spring Boot application are separate Docker containers.

The sidecar needed to know how to reach the application container.

### Solution

Added:

```text
--app-channel-address
order-app
```

Final relationship:

```text
order-app-dapr
      │
      │ Docker DNS
      ▼
order-app:8080
```

---

## Issue 5 — PowerShell JSON caused HTTP 400

### Error

Spring reported:

```text
HttpMessageNotReadableException
JSON parse error
Unexpected character ('i')
was expecting double-quote
```

### Cause

PowerShell command-line quoting changed the JSON payload.

### Solution

Use PowerShell's `ConvertTo-Json`:

```powershell
$body = @{
    id = 999
    customer_name = "TEST"
    product = "AC"
    amount = 100.00
    status = "NEW"
} | ConvertTo-Json

Invoke-RestMethod `
    -Uri "http://localhost:8080/consume" `
    -Method POST `
    -ContentType "application/json" `
    -Body $body
```

The direct `/consume` test then worked.

---

# 25. Issue 6 — Consumer Received CloudEvent Instead of Order

### Error

The producer successfully published:

```text
[PRODUCER] Order published successfully
```

But `/consume` returned HTTP 400.

Dapr logged:

```text
retriable error returned from app while processing pub/sub event
status code returned: 400
```

Spring Boot reported:

```text
Cannot deserialize value of type `java.lang.Long`
from String "cd504acc-ef75-4e95-b69a-11dd9568aa10"
```

### Cause

The consumer expected:

```json
{
  "id": 10,
  "customer_name": "Teju"
}
```

But Dapr delivered a CloudEvent:

```json
{
  "id": "cd504acc-ef75-4e95-b69a-11dd9568aa10",
  "data": {
    "id": 10,
    "customer_name": "Teju"
  }
}
```

Spring tried to map the CloudEvent's UUID `id` into:

```java
Long id;
```

which failed.

### Solution

Changed the consumer from:

```java
@RequestBody Order order
```

to:

```java
@RequestBody JsonNode cloudEvent
```

Then extracted:

```java
JsonNode data = cloudEvent.get("data");
```

and converted:

```java
Order order = objectMapper.treeToValue(data, Order.class);
```

This correctly separates:

```text
CloudEvent metadata
        +
Business Order data
```

---

# 26. Issue 7 — `ObjectMapper` Bean Not Found

### Error

```text
Parameter 1 of constructor in
OrderConsumerController
required a bean of type
ObjectMapper that could not be found
```

### Cause

The controller constructor attempted to inject an `ObjectMapper` bean, but the required bean was not available in the application context.

### Solution

Instead of constructor injection, the mapper was instantiated directly:

```java
private final ObjectMapper objectMapper = new ObjectMapper();
```

The constructor was reduced to:

```java
public OrderConsumerController(
        ConsumedOrderRepository orderRepository) {

    this.orderRepository = orderRepository;
}
```

---

# 27. Issue 8 — Jackson 2 vs Jackson 3 Package Mismatch

### Error

After changing the controller to use `JsonNode`, the application reported:

```text
Type definition error:
[simple type, class com.fasterxml.jackson.databind.JsonNode]
```

The root cause showed:

```text
tools.jackson.databind.exc.InvalidDefinitionException
```

and:

```text
jackson-databind-3.1.5
```

### Cause

The controller imported Jackson 2 packages:

```java
com.fasterxml.jackson.databind.JsonNode
com.fasterxml.jackson.databind.ObjectMapper
```

while the application runtime uses Jackson 3:

```java
tools.jackson.databind.JsonNode
tools.jackson.databind.ObjectMapper
```

### Solution

Changed imports to:

```java
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
```

This aligns the controller with the Jackson version used by Spring Boot 4.

---

# 28. Issue 9 — Offset Explorer Displayed Hexadecimal Data

### Symptom

Offset Explorer displayed:

```text
7B2264617461223A7B22616D6F756E74223A...
```

instead of readable JSON.

### Cause

Kafka stores message values as bytes, and Offset Explorer was displaying the bytes using hexadecimal representation.

### Solution / Interpretation

This is not a Kafka or Dapr failure.

For example:

```text
7B = {
22 = "
```

Therefore:

```text
7B2264617461223A
```

represents:

```text
{"data":
```

Offset Explorer can be configured to display the value as a string/UTF-8 representation depending on its message display/deserializer settings.

---

# 29. Issue 10 — Accessing MySQL from DBeaver

### Requirement

The MySQL database is inside Docker, while DBeaver runs on Windows.

### Solution

Expose MySQL:

```yaml
ports:
  - "3306:3306"
```

Then DBeaver connects using:

```text
Host:     localhost
Port:     3306
Database: ordersdb
Username: root
Password: root
```

Do not remove the persistent volume:

```yaml
mysql-data:/var/lib/mysql
```

Avoid:

```powershell
docker compose down -v
```

because `-v` removes the Docker volumes and can remove the stored MySQL data.

---

# 30. Final POC Verification Checklist

The POC is considered successfully validated when all of the following work:

```text
[✓] Docker Compose starts successfully

[✓] Zookeeper is running

[✓] Kafka is running

[✓] MySQL is running

[✓] Spring Boot order-app is running

[✓] Dapr sidecar is running

[✓] Dapr loads pubsub component

[✓] Dapr discovers orders-subscription

[✓] Kafka topic orders exists

[✓] Kafka topic has 3 partitions

[✓] Producer publishes through Dapr

[✓] Kafka receives the event

[✓] Dapr consumer group order-app-group is present

[✓] Dapr consumes the Kafka event

[✓] Dapr invokes POST /consume

[✓] Consumer extracts CloudEvent.data

[✓] Order is converted to Order object

[✓] Order is saved to MySQL

[✓] Kafka data can be inspected with Offset Explorer

[✓] MySQL data can be inspected with DBeaver
```

---

# 31. Final Architecture Summary

The completed POC demonstrates a clean event-driven integration:

```text
                    ┌──────────────┐
                    │    Client    │
                    └──────┬───────┘
                           │
                           │ POST /orders
                           ▼
                    ┌──────────────┐
                    │ Spring Boot  │
                    │ order-app    │
                    └──────┬───────┘
                           │
                           │ Dapr HTTP API
                           ▼
                    ┌──────────────┐
                    │ Dapr Sidecar │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │    Kafka     │
                    │ orders topic │
                    │  P0 P1 P2    │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │ Dapr Pub/Sub │
                    │ Subscription │
                    └──────┬───────┘
                           │
                           │ POST /consume
                           ▼
                    ┌──────────────┐
                    │ Spring Boot  │
                    │ Consumer     │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │    MySQL     │
                    │   ordersdb   │
                    └──────────────┘
```

The key architectural principle demonstrated by the POC is:

> **The application does not need to contain Kafka-specific producer/consumer logic. Dapr provides the Pub/Sub abstraction, while Kafka acts as the underlying event broker.**

This also makes the application architecture easier to evolve because the application communicates with Dapr through its API while Dapr handles the broker integration.

---

# 32. Useful Commands — Quick Reference

```powershell
# Start everything
docker compose up -d

# Build and start application
docker compose up -d --build order-app

# Recreate Dapr sidecar
docker compose up -d --force-recreate order-app-dapr

# Container status
docker ps

# Compose status
docker compose ps

# Application logs
docker logs -f order-app

# Dapr logs
docker logs -f order-app-dapr

# Kafka topics
docker exec kafka kafka-topics --list --bootstrap-server kafka:29092

# Describe orders topic
docker exec kafka kafka-topics --describe `
  --topic orders `
  --bootstrap-server kafka:29092

# MySQL shell
docker exec -it mysql mysql -uroot -proot ordersdb

# Dapr health
Invoke-RestMethod http://localhost:3500/v1.0/healthz

# Docker network test
docker run --rm `
  --network dapr-kafka-data-pipeline_app-network `
  curlimages/curl:latest `
  http://order-app:8080/
```

---

# 33. Tools Used for Observability / Verification

## Offset Explorer

Used to verify:

- Kafka cluster connectivity
- Kafka broker
- `orders` topic
- Partitions
- Kafka message offsets
- Consumer group
- Raw Kafka message payload

Connection from Windows:

```text
localhost:9092
```

## DBeaver

Used to verify:

- MySQL connectivity
- `ordersdb`
- Tables
- Persisted consumed orders

Connection:

```text
localhost:3306
```

---

# 34. POC Outcome

The POC successfully demonstrates the complete event lifecycle:

```text
Order Request
     ↓
Spring Boot
     ↓
Dapr Publish API
     ↓
Kafka
     ↓
Dapr Subscription
     ↓
Spring Boot Consumer
     ↓
CloudEvent.data
     ↓
Order Object
     ↓
MySQL
```

The main troubleshooting lessons were:

1. **Docker DNS and host networking are different.**
2. **Kafka needs appropriate internal and external listeners when both Docker and Windows clients connect.**
3. **Dapr component paths must match the mounted container path.**
4. **Dapr Pub/Sub events are delivered as CloudEvents by default in this setup.**
5. **The business payload is inside `CloudEvent.data`.**
6. **Kafka stores messages as bytes; GUI tools may display those bytes as hexadecimal.**
7. **Spring Boot 4 uses the Jackson 3 namespace shown in the runtime.**
8. **Persistent Docker volumes should be protected when recreating containers.**
9. **Dapr retries failed consumer deliveries, so a 400 response can result in repeated processing attempts.**
10. **Offset Explorer and DBeaver provide useful independent verification points for Kafka and MySQL.**

---

# 35. Final POC Status

```text
╔══════════════════════════════════════════════════════╗
║              DAPR + KAFKA POC                       ║
║                                                      ║
║  Spring Boot        ✓                                ║
║  Docker             ✓                                ║
║  Dapr               ✓                                ║
║  Kafka              ✓                                ║
║  Kafka Topic        ✓ orders                         ║
║  Partitions         ✓ 3                              ║
║  Consumer Group     ✓ order-app-group                ║
║  Subscription       ✓ /consume                       ║
║  CloudEvent         ✓ handled                        ║
║  MySQL              ✓ ordersdb                       ║
║  Offset Explorer    ✓ Kafka verification             ║
║  DBeaver            ✓ MySQL verification             ║
║                                                      ║
║              END-TO-END FLOW VERIFIED                ║
╚══════════════════════════════════════════════════════╝
```

