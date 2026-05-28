## Практическая работа №13. Вуйко Ярослава, ЭФМО-01-25
### Подключение к RabbitMQ. Отправка и получение сообщений. 27.05.2026


### Конфигурация docker-compose для RabbitMQ

Файл `deploy/rabbit/docker-compose.yml`:

```yaml
services:
  rabbitmq:
    image: rabbitmq:3.13-management-alpine
    container_name: rabbitmq
    ports:
      - "5672:5672"   
      - "15672:15672" 
    environment:
      - RABBITMQ_DEFAULT_USER=guest
      - RABBITMQ_DEFAULT_PASS=guest
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
```

### Используемые порты

 -5672 - AMQP (основной протокол для приложений)
 -15672 - Management UI (веб-интерфейс для администрирования)

### Запуск

```bash
cd deploy/rabbit
docker-compose up -d

cd deploy/tls
docker-compose up -d
```


## Формат сообщения

### Структура события

Сообщения публикуются в формате JSON. Структура события `TaskEvent`:

```json
{
    "event": "task.created",
    "task_id": "t_abc12345",
    "request_id": "pz13-001",
    "ts": "2026-05-27T12:00:00Z",
    "producer": "tasks-service",
    "version": "1.0"
}
```

### Описание полей

| Поле | Тип | Описание |
|------|-----|----------|
| `event` | string | Тип события (`task.created`) |
| `task_id` | string | Идентификатор созданной задачи |
| `request_id` | string | ID запроса для трассировки |
| `ts` | timestamp | Время создания события |
| `producer` | string | Имя сервиса-отправителя |
| `version` | string | Версия формата события |

### Почему выбран JSON

- Человеко-читаемый формат — легко отлаживать и анализировать
- Простота парсинга — нативная поддержка в Go (`encoding/json`)
- Расширяемость — можно добавлять новые поля без breaking changes
- Стандарт — формат для обмена данными в микросервисах


## Producer: публикация сообщений из tasks

### Где и когда публикуется сообщение

Сообщение публикуется после успешного создания задачи в базе данных, но до возврата ответа клиенту.

```go
func (s *TaskService) Create(ctx context.Context, token string, title, description, dueDate string) (models.Task, error) {
	requestID, _ := ctx.Value(logger.RequestIDKey{}).(string)

	log := s.logger.With(
		zap.String("request_id", requestID),
		zap.String("operation", "create"),
	)

	username, err := s.authClient.VerifyToken(ctx, token)
	if err != nil {
		return models.Task{}, fmt.Errorf("auth failed: %w", err)
	}

	task := models.Task{
		ID:          "t_" + uuid.New().String()[:8],
		Title:       title,
		Description: description,
		DueDate:     dueDate,
		Done:        false,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	if err := s.repo.Create(ctx, task); err != nil {
		return models.Task{}, err
	}

	log.Info("Task created",
		zap.String("task_id", task.ID),
		zap.String("username", username),
	)
  //ПУБЛИКАЦИЯ СООБЩЕНИЯ
	if s.rabbitClient != nil {

		queueName := os.Getenv("RABBIT_QUEUE_NAME")
		if queueName == "" {
			queueName = "task_events"
		}

		if err := s.rabbitClient.PublishTaskEvent(ctx, queueName, task.ID, requestID); err != nil {
			log.Warn("Failed to publish task created event", zap.Error(err))
		} else {
			log.Debug("Task created event published", zap.String("task_id", task.ID))
		}
	}

	return task, nil
}
```


## Consumer: Worker сервис


### Структура worker сервиса

```
.
│   Dockerfile
│   go.mod
│   go.sum
│   
├───cmd
│   └───worker
│           main.go
│           
└───internal
    ├───consumer
    │       consumer.go
    │       
    └───rabbitmq
            client.go
            
```

### Архитектура worker

Worker — отдельный сервис, который:
1. Подключается к RabbitMQ
2. Объявляет очередь `task_events` (те же параметры durable)
3. Устанавливает prefetch для контроля нагрузки
4. Подписывается на сообщения
5. Обрабатывает каждое сообщение и отправляет `ack`



### бработка сообщений и ack

```go
func handleMessage(msg amqp.Delivery) {
    // 1. Парсим JSON
    var event TaskEvent
    if err := json.Unmarshal(msg.Body, &event); err != nil {
        msg.Nack(false, true) // requeue при ошибке парсинга
        return
    }

    // 2. Обрабатываем событие (логируем)
    log.Printf("[EVENT] %s for task %s", event.Event, event.TaskID)

    // 3. Подтверждаем успешную обработку
    msg.Ack(false)
}
```




### Создание задачи
<img width="1382" height="652" alt="image" src="https://github.com/user-attachments/assets/1865dc6d-3595-42af-bb82-39784484be27" />



### Логи worker

```
[WORKER] 2026/05/27 19:25:47 Connecting to RabbitMQ at amqp://guest:guest@rabbitmq:5672/
[WORKER] 2026/05/27 19:25:47 RabbitMQ connected successfully. Queue: task_events
2026/05/27 19:25:47 Starting RabbitMQ consumer worker...
[WORKER] 2026/05/27 19:25:47 Prefetch set to 1
[WORKER] 2026/05/27 19:25:47 Started consuming from queue: task_events
[WORKER] 2026/05/27 19:25:47 Worker started. Waiting for messages on queue: task_events
[WORKER] 2026/05/27 19:26:32 Received message: {"event":"task.created","task_id":"t_0576b079","request_id":"017064a0-b89a-414a-a4cb-aada9e62e23f","ts":"2026-05-27T19:26:32.724421155Z","producer":"tasks-service","version":"1.0"}
[WORKER] 2026/05/27 19:26:32 [EVENT] task.created for task t_0576b079
```


### Проверка в RabbitMQ Management UI
<img width="1712" height="817" alt="image" src="https://github.com/user-attachments/assets/16dc25a3-4757-4b32-a7a8-c10e54998ef1" />

<img width="1277" height="292" alt="image" src="https://github.com/user-attachments/assets/d0e4dfa2-4d98-4d9e-978c-75d616908317" />


### Контрольные вопросы

1. Зачем нужен брокер сообщений, если есть HTTP?
HTTP — синхронный протокол, требующий немедленного ответа. Брокер сообщений позволяет:
- Асинхронную обработку (клиент не ждёт завершения фоновых задач)
- Слабую связанность компонентов (отправитель не знает о получателе)
- Буферизацию при временной недоступности получателя
- Гарантированную доставку

2. Что такое ack и зачем он нужен?
Ack (acknowledgement) — подтверждение от consumer'а, что сообщение успешно обработано. Без ack брокер не может быть уверен, что сообщение не потерялось. Ack гарантирует, что сообщение не будет потеряно при падении consumer'а.

3. Почему возможна повторная доставка сообщения?

Сообщение может быть доставлено повторно, если:
- Consumer не отправил ack в течение таймаута
- Consumer упал после получения, но до отправки ack
- Был отправлен nack с requeue=true
- Сеть между брокером и consumer'ом нестабильна

4. Что делает prefetch?

Prefetch ограничивает количество сообщений, которые consumer может получить до отправки ack. Это:
- Предотвращает перегрузку consumer'а
- Обеспечивает справедливое распределение сообщений между несколькими consumer'ами
- Позволяет контролировать потребление ресурсов

5. Чем очередь durable отличается от non-durable?
Durable очередь сохраняется после перезапуска RabbitMQ (метаданные очереди сохраняются на диск). Non-durable очередь удаляется при рестарте брокера. Сообщения в durable очереди также могут быть persistent (сохраняться на диск) или transient (храниться только в памяти).


### Вывод

В результате практического занятия успешно реализована асинхронная коммуникация между сервисами через RabbitMQ:

1. RabbitMQ поднят в Docker с доступом через порты 5672 и 15672
2. События публикуются в формате JSON с полями event, task_id, request_id, ts, producer, version
3. Публикация происходит после создания задачи в БД (best effort)
4. Worker потребляет сообщения с ручным ack и prefetch=1
