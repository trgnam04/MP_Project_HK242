package Deserializer;

import com.events.ClickEvent;
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

public class ClickDatasetDeserializationSchema implements DeserializationSchema<ClickEvent>, Serializable {
    private transient Schema schema;
    private transient GenericDatumReader<GenericRecord> reader;
    private transient BinaryDecoder decoder;

    public ClickDatasetDeserializationSchema() {
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        this.schema = ClickEvent.getClassSchema();
        this.reader = new GenericDatumReader<>(schema);
    }

    @Override
    public ClickEvent deserialize(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            throw new IOException("Received empty byte array for deserialization");
        }

        byte[] messageContent = new byte[bytes.length - 5];
        System.arraycopy(bytes, 5, messageContent, 0, messageContent.length);

        InputStream inputStream = new ByteArrayInputStream(messageContent);
        decoder = DecoderFactory.get().binaryDecoder(inputStream, decoder);
        GenericRecord record = reader.read(null, decoder);

        return ClickEvent.newBuilder()
                .setEvent(record.get("event") != null ? record.get("event").toString() : null)
                .setSessionId(record.get("sessionId") != null ? record.get("sessionId").toString() : null)
                .setTimestamp(record.get("timestamp") != null ? record.get("timestamp").toString() : null)
                .setCategory(record.get("category") != null ? record.get("category").toString() : null)
                .setItemId(record.get("itemId") != null ? record.get("itemId").toString() : null)
                .build();
    }

    @Override
    public boolean isEndOfStream(ClickEvent clickDataset) {
        return false;
    }

    @Override
    public TypeInformation<ClickEvent> getProducedType() {
        return TypeInformation.of(ClickEvent.class);
    }
}