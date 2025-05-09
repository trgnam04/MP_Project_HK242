package Deserializer;

import com.events.TopupEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class TopUpDeserializationScheme implements DeserializationSchema<TopupEvent> {
    private transient Schema schema;
    private transient GenericDatumReader<GenericRecord> reader;
    private transient BinaryDecoder decoder;

    @Override
    public void open(InitializationContext context) {
        this.schema = TopupEvent.getClassSchema();
        this.reader = new GenericDatumReader<>(schema);
    }

    @Override
    public TopupEvent deserialize(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length <= 5) {
            throw new IOException("Invalid or empty byte array received for deserialization.");
        }

        // Bỏ qua 5 byte đầu (magic byte + schema ID)
        byte[] content = new byte[bytes.length - 5];
        System.arraycopy(bytes, 5, content, 0, content.length);

        if (reader == null) {
            schema = TopupEvent.getClassSchema();
            reader = new GenericDatumReader<>(schema);
        }

        InputStream inputStream = new ByteArrayInputStream(content);
        decoder = DecoderFactory.get().binaryDecoder(inputStream, decoder);
        GenericRecord record = reader.read(null, decoder);

        return TopupEvent.newBuilder()
                .setCreatedAt(Objects.toString(record.get("createdAt"), null))
                .setUpdatedAt(Objects.toString(record.get("updatedAt"), null))
                .setId(Objects.toString(record.get("id"), null))
                .setCode(Objects.toString(record.get("code"), null))
                .setDescription(Objects.toString(record.get("description"), null))
                .setPhoneNumber(Objects.toString(record.get("phoneNumber"), null))
                .setPartnerTransId(Objects.toString(record.get("partnerTransId"), null))
                .setSupplierTransId(Objects.toString(record.get("supplierTransId"), null))
                .setJobId(Objects.toString(record.get("jobId"), null))
                .setIsPostpaid((Integer) record.get("isPostpaid"))
                .setTotalAmount((Integer) record.get("totalAmount"))
                .setDiscountRate((Integer) record.get("discountRate"))
                .setBeforeCredit((Integer) record.get("beforeCredit"))
                .setAfterCredit((Integer) record.get("afterCredit"))
                .setIp(Objects.toString(record.get("ip"), null))
                .setType((Integer) record.get("type"))
                .setStatus((Integer) record.get("status"))
                .setSupplier(Objects.toString(record.get("supplier"), null))
                .setMobileData(Objects.toString(record.get("mobileData"), null))
                .setMerchantUser(Objects.toString(record.get("merchantUser"), null))
                .setMerchantSubscription(Objects.toString(record.get("merchantSubscription"), null))
                .build();
    }

    @Override
    public boolean isEndOfStream(TopupEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<TopupEvent> getProducedType() {
        return TypeInformation.of(TopupEvent.class);
    }
}
