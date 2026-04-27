#!/bin/sh

echo "Ожидание готовности Flink JobManager..."

until curl -s http://jobmanager:8081/overview > /dev/null 2>&1; do
    echo "Flink JobManager ещё не готов, ждём 5 секунд..."
    sleep 5
done

echo ""
echo "Flink JobManager готов!"
echo ""

JAR_FILE="/opt/flink/jobs/flink-streaming-job-1.0-SNAPSHOT.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "ОШИБКА: JAR-файл не найден: $JAR_FILE"
    ls -la /opt/flink/jobs/
    exit 1
fi

echo "Загружаем JAR: $JAR_FILE"

UPLOAD_RESPONSE=$(curl -s -X POST \
    -H "Expect:" \
    -F "jarfile=@${JAR_FILE}" \
    http://jobmanager:8081/jars/upload)

echo "Ответ загрузки: $UPLOAD_RESPONSE"

JAR_ID=$(echo "$UPLOAD_RESPONSE" | grep -o '"filename":"[^"]*"' | cut -d'"' -f4 | xargs basename)

if [ -z "$JAR_ID" ]; then
    echo "ОШИБКА: Не удалось загрузить JAR"
    exit 1
fi

echo "JAR загружен, ID: $JAR_ID"

SUBMIT_RESPONSE=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d '{
        "entryClass": "com.bigdata.flink.StreamingJob",
        "parallelism": "2",
        "programArgs": ""
    }' \
    "http://jobmanager:8081/jars/${JAR_ID}/run")

echo ""
echo "Ответ запуска: $SUBMIT_RESPONSE"

# Проверяем успешность
if echo "$SUBMIT_RESPONSE" | grep -q '"jobid"'; then
    JOB_ID=$(echo "$SUBMIT_RESPONSE" | grep -o '"jobid":"[^"]*"' | cut -d'"' -f4)
    echo ""
    echo "ДЖОБА УСПЕШНО ЗАПУЩЕНА!"
    echo "Job ID: $JOB_ID"
    echo "Flink Dashboard: http://localhost:8081"
else
    echo ""
    echo "ОШИБКА ЗАПУСКА ДЖОБЫ"
    echo "Ответ: $SUBMIT_RESPONSE"
fi

echo ""
echo "Для выхода нажмите Ctrl+C"
tail -f /dev/null