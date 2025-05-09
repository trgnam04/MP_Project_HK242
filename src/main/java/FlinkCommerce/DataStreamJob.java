package FlinkCommerce;

import Deserializer.BuyDatasetDeserializationSchema;
import Deserializer.ClickDatasetDeserializationSchema;
import Deserializer.TopUpDeserializationScheme;
import com.events.TopupEvent;
import com.events.BuyEvent;
import com.events.ClickEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamJob {
    private static final String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";
    private static final String username = "postgres";
    private static final String password = "postgres";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        JdbcExecutionOptions jobExecutionOptions = new JdbcExecutionOptions.Builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(username)
                .withPassword(password)
                .build();

        // Topup events stream
        KafkaSource<TopupEvent> topupStream = KafkaSource.<TopupEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("topup_events")
                .setGroupId("Topup-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new TopUpDeserializationScheme())
                .build();

        DataStream<TopupEvent> topupEvents = env.fromSource(topupStream, WatermarkStrategy.noWatermarks(), "Topup Events")
                .filter((FilterFunction<TopupEvent>) value ->
                        value != null &&
                        value.getPhoneNumber() != null &&
                        !value.getPhoneNumber().toString().isEmpty()
                );

        topupEvents.print();

        // Create table with created_at and updated_at
        topupEvents.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS topup_events (" +
                        "idx SERIAL PRIMARY KEY, " +
                        "created_at VARCHAR(255), " +
                        "updated_at VARCHAR(255), " +
                        "id VARCHAR(255), " +
                        "code VARCHAR(255), " +
                        "description TEXT, " +
                        "phone_number VARCHAR(255), " +
                        "partner_trans_id VARCHAR(255), " +
                        "supplier_trans_id VARCHAR(255), " +
                        "job_id VARCHAR(255), " +
                        "is_postpaid INT, " +
                        "total_amount INT, " +
                        "discount_rate INT, " +
                        "before_credit INT, " +
                        "after_credit INT, " +
                        "ip VARCHAR(255), " +
                        "type INT, " +
                        "status INT, " +
                        "supplier VARCHAR(255), " +
                        "mobile_data VARCHAR(255), " +
                        "merchant_user VARCHAR(255), " +
                        "merchant_subscription VARCHAR(255)" +
                        ")",
                (JdbcStatementBuilder<TopupEvent>) (ps, t) -> {
                    // no-op for DDL
                },
                jobExecutionOptions,
                connectionOptions
        )).name("Create Topup dataset table");

        // Insert into table
        topupEvents.addSink(JdbcSink.sink(
                "INSERT INTO topup_events (" +
                        "created_at, updated_at, id, code, description, phone_number, " +
                        "partner_trans_id, supplier_trans_id, job_id, is_postpaid, total_amount, " +
                        "discount_rate, before_credit, after_credit, ip, type, status, " +
                        "supplier, mobile_data, merchant_user, merchant_subscription" +
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (JdbcStatementBuilder<TopupEvent>) (ps, t) -> {
                    ps.setString(1, t.getCreatedAt() != null ? t.getCreatedAt().toString() : null);
                    ps.setString(2, t.getUpdatedAt() != null ? t.getUpdatedAt().toString() : null);
                    ps.setString(3, t.getId() != null ? t.getId().toString() : null);
                    ps.setString(4, t.getCode() != null ? t.getCode().toString() : null);
                    ps.setString(5, t.getDescription() != null ? t.getDescription().toString() : null);
                    ps.setString(6, t.getPhoneNumber() != null ? t.getPhoneNumber().toString() : null);
                    ps.setString(7, t.getPartnerTransId() != null ? t.getPartnerTransId().toString() : null);
                    ps.setString(8, t.getSupplierTransId() != null ? t.getSupplierTransId().toString() : null);
                    ps.setString(9, t.getJobId() != null ? t.getJobId().toString() : null);
                    ps.setInt(10, t.getIsPostpaid());
                    ps.setInt(11, t.getTotalAmount());
                    ps.setInt(12, t.getDiscountRate());
                    ps.setInt(13, t.getBeforeCredit());
                    ps.setInt(14, t.getAfterCredit());
                    ps.setString(15, t.getIp() != null ? t.getIp().toString() : null);
                    ps.setInt(16, t.getType());
                    ps.setInt(17, t.getStatus());
                    ps.setString(18, t.getSupplier() != null ? t.getSupplier().toString() : null);
                    ps.setString(19, t.getMobileData() != null ? t.getMobileData().toString() : null);
                    ps.setString(20, t.getMerchantUser() != null ? t.getMerchantUser().toString() : null);
                    ps.setString(21, t.getMerchantSubscription() != null ? t.getMerchantSubscription().toString() : null);
                },
                jobExecutionOptions,
                connectionOptions
        )).name("Insert Topup dataset");

        ///-----------------------------BuyDatset---------------------------------------///
        KafkaSource<BuyEvent> buyStream = KafkaSource.<BuyEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("buy_events")
                .setGroupId("Buy-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new BuyDatasetDeserializationSchema())
                .build();

        DataStream<BuyEvent> buyEvents = env.fromSource(buyStream, WatermarkStrategy.noWatermarks(), "Buy Events")
                .filter(new FilterFunction<BuyEvent>() {
                    @Override
                    public boolean filter(BuyEvent value) throws Exception {
                        return value != null &&
                               value.getSessionId() != null &&
                               !value.getSessionId().toString().isEmpty();
                    }
                });

        buyEvents.print();

        buyEvents.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS buy_events ( " +
                        "idx SERIAL PRIMARY KEY, " +
                        "session_id VARCHAR(255), " +
                        "timestamp VARCHAR(255), " +
                        "item_id VARCHAR(255), " +
                        "price INT, " +
                        "quantity INT " +
                        ")",
                (JdbcStatementBuilder<BuyEvent>) (ps, t) -> {},
                jobExecutionOptions,
                connectionOptions
        )).name("Create Buy dataset table");

        buyEvents.addSink(JdbcSink.sink(
                "INSERT INTO buy_events (session_id, timestamp, item_id, price, quantity) " +
                        "VALUES (?, ?, ?, ?, ?) ",
                (JdbcStatementBuilder<BuyEvent>) (ps, t) -> {
                    ps.setString(1, t.getSessionId() != null ? t.getSessionId().toString() : null);
                    ps.setString(2, t.getTimestamp() != null ? t.getTimestamp().toString() : null);
                    ps.setString(3, t.getItemId() != null ? t.getItemId().toString() : null);
                    ps.setInt(4, t.getPrice());
                    ps.setInt(5, t.getQuantity());
                },
                jobExecutionOptions,
                connectionOptions
        )).name("Insert Buy dataset");

        ///-----------------------------ClickDatset---------------------------------------///
        KafkaSource<ClickEvent> clickStream = KafkaSource.<ClickEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("click_events")
                .setGroupId("Click-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new ClickDatasetDeserializationSchema())
                .build();

        DataStream<ClickEvent> clickEvents = env.fromSource(clickStream, WatermarkStrategy.noWatermarks(), "Click Events")
                .filter(new FilterFunction<ClickEvent>() {
                    @Override
                    public boolean filter(ClickEvent value) throws Exception {
                        return value != null &&
                               value.getSessionId() != null &&
                               !value.getSessionId().toString().isEmpty();
                    }
                });

        clickEvents.print();

        clickEvents.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS click_events ( " +
                        "idx SERIAL PRIMARY KEY, " +
                        "session_id VARCHAR(255), " +
                        "timestamp VARCHAR(255), " +
                        "item_id VARCHAR(255), " +
                        "category VARCHAR(255) " +
                        ")",
                (JdbcStatementBuilder<ClickEvent>) (ps, t) -> {},
                jobExecutionOptions,
                connectionOptions
        )).name("Create Click dataset table");

        clickEvents.addSink(JdbcSink.sink(
                "INSERT INTO click_events (session_id, timestamp, item_id, category) " +
                        "VALUES (?, ?, ?, ?) ",
                (JdbcStatementBuilder<ClickEvent>) (ps, t) -> {
                    ps.setString(1, t.getSessionId() != null ? t.getSessionId().toString() : null);
                    ps.setString(2, t.getTimestamp() != null ? t.getTimestamp().toString() : null);
                    ps.setString(3, t.getItemId() != null ? t.getItemId().toString() : null);
                    ps.setString(4, t.getCategory() != null ? t.getCategory().toString() : null);
                },
                jobExecutionOptions,
                connectionOptions
        )).name("Insert Click dataset");

        env.execute("Đồ án đa ngành");
    }
}
