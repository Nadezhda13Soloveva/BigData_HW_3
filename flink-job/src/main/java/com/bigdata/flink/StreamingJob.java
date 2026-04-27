package com.bigdata.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamingJob {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("M/d/yyyy");

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 1. Источник Apache Kafka
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("mock_data_topic")
                .setGroupId("flink-snowflake-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka-Source"
        );

        // 2. трансформация: json -> pojo SaleRecord
        DataStream<SaleRecord> saleRecordStream = kafkaStream
                .flatMap(new JsonToSaleRecordMapper())
                .name("Json-To-SaleRecord-Mapper");

        // 3. сохранение в PostgreSQL
        addCustomerSink(saleRecordStream);
        addSellerSink(saleRecordStream);
        addProductSink(saleRecordStream);
        addStoreSink(saleRecordStream);
        addSupplierSink(saleRecordStream);
        addFactSink(saleRecordStream);

        // 4. запуск Flink Job
        env.execute("Kafka -> Flink -> PostgreSQL: Snowflake Schema Streaming");
    }

    // mapper: преобразование json строки в объект SaleRecord
    public static class JsonToSaleRecordMapper implements FlatMapFunction<String, SaleRecord> {
        @Override
        public void flatMap(String value, org.apache.flink.util.Collector<SaleRecord> out) {
            try {
                JsonNode json = objectMapper.readTree(value);
                SaleRecord record = new SaleRecord();

                // Измерение: Customer
                if (json.hasNonNull("sale_customer_id")) {
                    record.customerId = json.get("sale_customer_id").asInt();
                    record.customerFirstName = json.get("customer_first_name").asText("");
                    record.customerLastName = json.get("customer_last_name").asText("");
                    record.customerAge = json.get("customer_age").asInt(0);
                    record.customerEmail = json.get("customer_email").asText("");
                    record.customerCountry = json.get("customer_country").asText("");
                    record.customerPostalCode = json.get("customer_postal_code").asText("");
                    record.customerPetType = json.get("customer_pet_type").asText("");
                    record.customerPetName = json.get("customer_pet_name").asText("");
                    record.customerPetBreed = json.get("customer_pet_breed").asText("");
                }

                // Измерение: Seller
                if (json.hasNonNull("sale_seller_id")) {
                    record.sellerId = json.get("sale_seller_id").asInt();
                    record.sellerFirstName = json.get("seller_first_name").asText("");
                    record.sellerLastName = json.get("seller_last_name").asText("");
                    record.sellerEmail = json.get("seller_email").asText("");
                    record.sellerCountry = json.get("seller_country").asText("");
                    record.sellerPostalCode = json.get("seller_postal_code").asText("");
                }

                // Измерение: Product
                if (json.hasNonNull("sale_product_id")) {
                    record.productId = json.get("sale_product_id").asInt();
                    record.productName = json.get("product_name").asText("");
                    record.productCategory = json.get("product_category").asText("");
                    record.productPrice = json.get("product_price").asDouble(0.0);
                    record.productQuantity = json.get("product_quantity").asInt(0);
                    record.productWeight = json.get("product_weight").asDouble(0.0);
                    record.productColor = json.get("product_color").asText("");
                    record.productSize = json.get("product_size").asText("");
                    record.productBrand = json.get("product_brand").asText("");
                    record.productMaterial = json.get("product_material").asText("");
                    record.productDescription = json.get("product_description").asText("");
                    record.productRating = json.get("product_rating").asDouble(0.0);
                    record.productReviews = json.get("product_reviews").asInt(0);
                    record.productReleaseDate = parseDateSafely(json, "product_release_date");
                    record.productExpiryDate = parseDateSafely(json, "product_expiry_date");
                }

                // Измерения (Store и Supplier)
                record.storeName = json.get("store_name").asText("");
                record.storeLocation = json.get("store_location").asText("");
                record.storeCity = json.get("store_city").asText("");
                record.storeState = json.get("store_state").asText("");
                record.storeCountry = json.get("store_country").asText("");
                record.storePhone = json.get("store_phone").asText("");
                record.storeEmail = json.get("store_email").asText("");

                record.supplierName = json.get("supplier_name").asText("");
                record.supplierContact = json.get("supplier_contact").asText("");
                record.supplierEmail = json.get("supplier_email").asText("");
                record.supplierPhone = json.get("supplier_phone").asText("");
                record.supplierAddress = json.get("supplier_address").asText("");
                record.supplierCity = json.get("supplier_city").asText("");
                record.supplierCountry = json.get("supplier_country").asText("");

                // Факты
                record.sourceSaleId = json.get("id").asLong(0);
                record.saleDate = parseDateSafely(json, "sale_date");
                record.saleQuantity = json.get("sale_quantity").asInt(0);
                record.saleTotalPrice = json.get("sale_total_price").asDouble(0.0);

                out.collect(record);

            } catch (Exception e) {
                LOG.error("Failed to parse JSON message: {}", value, e);
            }
        }
        
        private Date parseDateSafely(JsonNode json, String fieldName) {
            if (json.hasNonNull(fieldName)) {
                String dateStr = json.get(fieldName).asText().trim();
                if (!dateStr.isEmpty()) {
                    try {
                        return Date.valueOf(LocalDate.parse(dateStr, DATE_FORMATTER));
                    } catch (DateTimeParseException e) {
                        LOG.warn("Could not parse date '{}' from field '{}'", dateStr, fieldName);
                    }
                }
            }
            return null;
        }
    }

    //Sink'и для измерений    
    private static void addCustomerSink(DataStream<SaleRecord> stream) {
        stream.filter(r -> r.customerId != 0)
              .addSink(JdbcSink.sink(
                "INSERT INTO dim_customer (customer_id, first_name, last_name, age, email, country, postal_code, pet_type, pet_name, pet_breed) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (customer_id) DO UPDATE SET " +
                "first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, age = EXCLUDED.age, " +
                "email = EXCLUDED.email, country = EXCLUDED.country, postal_code = EXCLUDED.postal_code, " +
                "pet_type = EXCLUDED.pet_type, pet_name = EXCLUDED.pet_name, pet_breed = EXCLUDED.pet_breed",
                (ps, r) -> {
                    ps.setInt(1, r.customerId);
                    ps.setString(2, r.customerFirstName);
                    ps.setString(3, r.customerLastName);
                    ps.setInt(4, r.customerAge);
                    ps.setString(5, r.customerEmail);
                    ps.setString(6, r.customerCountry);
                    ps.setString(7, r.customerPostalCode);
                    ps.setString(8, r.customerPetType);
                    ps.setString(9, r.customerPetName);
                    ps.setString(10, r.customerPetBreed);
                },
                defaultExecOptions(),
                defaultConnectionOptions()
              )).name("Sink: dim_customer");
    }

    private static void addSellerSink(DataStream<SaleRecord> stream) {
        stream.filter(r -> r.sellerId != 0)
              .addSink(JdbcSink.sink(
                "INSERT INTO dim_seller (seller_id, first_name, last_name, email, country, postal_code) " +
                "VALUES (?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (seller_id) DO UPDATE SET " +
                "first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, email = EXCLUDED.email, " +
                "country = EXCLUDED.country, postal_code = EXCLUDED.postal_code",
                (ps, r) -> {
                    ps.setInt(1, r.sellerId);
                    ps.setString(2, r.sellerFirstName);
                    ps.setString(3, r.sellerLastName);
                    ps.setString(4, r.sellerEmail);
                    ps.setString(5, r.sellerCountry);
                    ps.setString(6, r.sellerPostalCode);
                },
                defaultExecOptions(),
                defaultConnectionOptions()
              )).name("Sink: dim_seller");
    }

    private static void addProductSink(DataStream<SaleRecord> stream) {
        stream.filter(r -> r.productId != 0)
              .addSink(JdbcSink.sink(
                "INSERT INTO dim_product (product_id, name, category, price, stock_quantity, weight, color, size, brand, material, description, rating, reviews, release_date, expiry_date) " +
                "VALUES (?, ?, ?, CAST(? AS NUMERIC(10,2)), ?, CAST(? AS NUMERIC(5,2)), ?, ?, ?, ?, ?, CAST(? AS NUMERIC(3,1)), ?, ?, ?) " +
                "ON CONFLICT (product_id) DO UPDATE SET " +
                "name = EXCLUDED.name, category = EXCLUDED.category, price = EXCLUDED.price, stock_quantity = EXCLUDED.stock_quantity, " +
                "weight = EXCLUDED.weight, brand = EXCLUDED.brand, rating = EXCLUDED.rating, description = EXCLUDED.description",
                (ps, r) -> {
                    ps.setInt(1, r.productId);
                    ps.setString(2, r.productName);
                    ps.setString(3, r.productCategory);
                    ps.setDouble(4, r.productPrice); // CAST внутри SQL
                    ps.setInt(5, r.productQuantity);
                    ps.setDouble(6, r.productWeight); // CAST внутри SQL
                    ps.setString(7, r.productColor);
                    ps.setString(8, r.productSize);
                    ps.setString(9, r.productBrand);
                    ps.setString(10, r.productMaterial);
                    ps.setString(11, r.productDescription);
                    ps.setDouble(12, r.productRating); // CAST внутри SQL
                    ps.setInt(13, r.productReviews);
                    ps.setDate(14, r.productReleaseDate);
                    ps.setDate(15, r.productExpiryDate);
                },
                defaultExecOptions(),
                defaultConnectionOptions()
              )).name("Sink: dim_product");
    }

    private static void addStoreSink(DataStream<SaleRecord> stream) {
        stream.filter(r -> !r.storeName.isEmpty())
              .addSink(JdbcSink.sink(
                "INSERT INTO dim_store (name, location, city, state, country, phone, email) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (name) DO UPDATE SET " +
                "location = EXCLUDED.location, city = EXCLUDED.city, state = EXCLUDED.state, " +
                "country = EXCLUDED.country, phone = EXCLUDED.phone, email = EXCLUDED.email",
                (ps, r) -> {
                    ps.setString(1, r.storeName);
                    ps.setString(2, r.storeLocation);
                    ps.setString(3, r.storeCity);
                    ps.setString(4, r.storeState);
                    ps.setString(5, r.storeCountry);
                    ps.setString(6, r.storePhone);
                    ps.setString(7, r.storeEmail);
                },
                defaultExecOptions(),
                defaultConnectionOptions()
              )).name("Sink: dim_store");
    }

    private static void addSupplierSink(DataStream<SaleRecord> stream) {
        stream.filter(r -> !r.supplierName.isEmpty())
              .addSink(JdbcSink.sink(
                "INSERT INTO dim_supplier (name, contact, email, phone, address, city, country) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (name) DO UPDATE SET " +
                "contact = EXCLUDED.contact, email = EXCLUDED.email, phone = EXCLUDED.phone, " +
                "address = EXCLUDED.address, city = EXCLUDED.city, country = EXCLUDED.country",
                (ps, r) -> {
                    ps.setString(1, r.supplierName);
                    ps.setString(2, r.supplierContact);
                    ps.setString(3, r.supplierEmail);
                    ps.setString(4, r.supplierPhone);
                    ps.setString(5, r.supplierAddress);
                    ps.setString(6, r.supplierCity);
                    ps.setString(7, r.supplierCountry);
                },
                defaultExecOptions(),
                defaultConnectionOptions()
              )).name("Sink: dim_supplier");
    }

    // Sink для таблицы фактов
    private static void addFactSink(DataStream<SaleRecord> stream) {
        stream.filter(r -> r.sourceSaleId != 0)
              .addSink(JdbcSink.sink(
                "INSERT INTO fact_sales (source_sale_id, sale_date, customer_id, seller_id, product_id, store_id, supplier_id, quantity, total_price) " +
                "VALUES (?, ?, ?, ?, ?, " +
                "   (SELECT store_id FROM dim_store WHERE name = ?), " +
                "   (SELECT supplier_id FROM dim_supplier WHERE name = ?), " +
                "   ?, CAST(? AS NUMERIC(12,2)))",  //явный cast для total_price
                (ps, r) -> {
                    ps.setLong(1, r.sourceSaleId);
                    ps.setDate(2, r.saleDate);
                    ps.setInt(3, r.customerId);
                    ps.setInt(4, r.sellerId);
                    ps.setInt(5, r.productId);
                    // Параметры для подзапросов
                    ps.setString(6, r.storeName);
                    ps.setString(7, r.supplierName);
                    ps.setInt(8, r.saleQuantity);
                    ps.setDouble(9, r.saleTotalPrice); // cast внутри sql
                },
                defaultExecOptions(),
                defaultConnectionOptions()
              )).name("Sink: fact_sales");
    }

    // доп методы для конфигурации JDBC
    private static JdbcExecutionOptions defaultExecOptions() {
        return JdbcExecutionOptions.builder()
                .withBatchSize(50)  // размер батча
                .withBatchIntervalMs(200)   // интервал сброса батча
                .withMaxRetries(3)   // повторы при ошибках
                .build();
    }

    private static JdbcConnectionOptions defaultConnectionOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://postgres:5432/bigdata")
                .withDriverName("org.postgresql.Driver")
                .withUsername("bigdata")
                .withPassword("bigdata123")
                .build();
    }
}