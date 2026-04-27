# Анализ больших данных - Лабораторная работа №2: Streaming processing с помощью Flink

---

**Работу выполнил(а):** Соловьева Надежда Сергеевна

**Группа:** М8О-308Б-23

---

## 1. Цель работы

Необходимо реализовать потоковую обработку данных с помощью Flink, который читает топик Kafka, трансформирует данные в режиме streaming в модель звезда и пишет результат в PostgreSQL. Данные в Kafka-топиках хранятся в формате json. Данные в топик kafka нужно отправлять самостоятельно, эмулируя источник данных.

## 2. Алгоритм работы

1. Клонируете к себе этот репозиторий.
2. Устанавливаете инструмент для работы с запросами SQL (рекомендую DBeaver).
3. Устанавливаете базу данных PostgreSQL (рекомендую установку через docker).
4. Устанавливаете Apache Flink (рекомендую установку через Docker).
5. Устанавливаете Apache Kafka (рекомендую установку через Docker).
6. Скачиваете файлы с исходными данными mock_data( * ).csv, где ( * ) номера файлов. Всего 10 файлов, каждый по 1000 строк.
7. Реализуете приложение, которое каждую строчку из исходных csv-файлов преобразует в json и отправляет в виде сообщения в Kafka-топик.
8. Реализуете приложение на Flink, которое читает Kafka-топик, преобразует данные в модель звезда и сохраняет в PostgreSQL в режиме streaming.
9. Проверяете конечные данные в PostgreSQL.
10. Отправляете работу на проверку лаборантам.

Что должно быть результатом работы?

1. Репозиторий, в котором есть исходные данные mock_data().csv, где () номера файлов. Всего 10 файлов, каждый по 1000 строк.
2. Файл docker-compose.yml с установкой PostgreSQL, Flink, Kafka и запуском приложения, которое из файлов mock_data(*).csv создает сообщения json в Kafka.
3. Инструкция, как запускать Flink-джобу и приложение для отправки данных в Kafka для проверки лабораторной работы.
4. Код Apache Flink для трансформации данных в режиме streaming.

----

## 3. Решение

Для реализации работы использовата звезда-схема (см [./init](init)), созданная в [Лабораторной работе №1](https://github.com/Nadezhda13Soloveva/BigData_HW_1).
Архитектура решения представляет собой streaming-пайплайн, состоящий из трёх основных слоёв:
* **Слой источников данных:** В качестве источника данных выступают CSV-файлы, содержащие денормализованные данные о продажах. Для эмуляции потокового поступления данных разработан Kafka Producer, который: последовательно читает все CSV, преобразует каждую строку в JSON-объект, отправляет сообщения в топик Kafka mock_data_topic с уникальным ключом.
* **Слой потоковой обработки:** Flink-приложение читает данные из Kafka, каждое JSON-сообщение преобразует в Java-объект SaleRecord, содержащий все поля исходных данных. Далее происходит трансформация в Snowflake, из денормализованной записи выделяются Customer, Seller, Product, Store, Supplier и факты. 
* **Слой хранения данных:** Целевая база данных — PostgreSQL 16 (Alpine), в которой создана Snowflake-схема.


Проект настроен на полностью автоматический запуск всей цепочки обработки при выполнении команды `docker-compose up -d`.


Ниже представлены несколько запросов, демонстрирующий корректную обработку данных:

```
hopeee@hopeeenad:~/repository/BigDataFlink$ docker exec -it postgres_db psql -U bigdata -d bigdata -c "
SELECT 'dim_customer' as таблица, COUNT(*) as записей FROM dim_customer
UNION ALL SELECT 'dim_seller', COUNT(*) FROM dim_seller
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_store', COUNT(*) FROM dim_store
UNION ALL SELECT 'dim_supplier', COUNT(*) FROM dim_supplier
UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales
ORDER BY таблица;
"
   таблица    | записей
--------------+---------
 dim_customer |    1000
 dim_product  |    1000
 dim_seller   |    1000
 dim_store    |     383
 dim_supplier |     383
 fact_sales   |   10000
(6 rows)

hopeee@hopeeenad:~/repository/BigDataFlink$ docker exec -it postgres_db psql -U bigdata -d bigdata -c "
SELECT
    c.country,
    COUNT(*) as sales,
    SUM(f.total_price) as revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.country
ORDER BY revenue DESC
LIMIT 10;
"
   country   | sales |  revenue
-------------+-------+-----------
 China       |  1770 | 451918.73
 Indonesia   |  1120 | 280610.79
 Philippines |   460 | 119883.90
 Poland      |   460 | 115900.09
 Russia      |   470 | 115530.01
 Portugal    |   360 |  91728.89
 France      |   350 |  86191.46
 Brazil      |   330 |  84045.00
 Sweden      |   280 |  70079.92
 Japan       |   230 |  59454.10
(10 rows)
```