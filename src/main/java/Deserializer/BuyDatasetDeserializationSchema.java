package Deserializer;

import com.events.BuyEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

public class BuyDatasetDeserializationSchema implements DeserializationSchema<BuyEvent>, Serializable {
    private transient Schema schema;
    private transient GenericDatumReader<GenericRecord> reader;
    private transient BinaryDecoder decoder;

    public BuyDatasetDeserializationSchema() {
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        schema = BuyEvent.getClassSchema();
        reader = new GenericDatumReader<>(schema);
    }

    @Override
    public BuyEvent deserialize(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            throw new IOException("Received empty byte array for deserialization");
        }

        byte[] messageContent = new byte[bytes.length - 5];
        System.arraycopy(bytes, 5, messageContent, 0, messageContent.length);

        InputStream inputStream = new ByteArrayInputStream(messageContent);
        decoder = DecoderFactory.get().binaryDecoder(inputStream, decoder);
        GenericRecord record = reader.read(null, decoder);

        return BuyEvent.newBuilder()
                .setEvent(record.get("event") != null ? record.get("event").toString() : null)
                .setSessionId(record.get("sessionId") != null ? record.get("sessionId").toString() : null)
                .setTimestamp(record.get("timestamp") != null ? record.get("timestamp").toString() : null)
                .setItemId(record.get("itemId") != null ? record.get("itemId").toString() : null)
                .setPrice(record.get("price") != null ? (Integer) record.get("price") : 0)
                .setQuantity(record.get("quantity") != null ? (Integer) record.get("quantity") : 0)
                .build();
    }

    @Override
    public boolean isEndOfStream(BuyEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<BuyEvent> getProducedType() {
        return TypeInformation.of(BuyEvent.class);
    }
}
