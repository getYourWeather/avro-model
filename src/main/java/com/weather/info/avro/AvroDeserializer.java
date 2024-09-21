package com.weather.info.avro;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroDeserializer<T extends GenericRecord> implements Deserializer<T>, Serializable {

    private static final long serialVersionUID = 9171165257671518562L;

    static Logger logger = LoggerFactory.getLogger(AvroDeserializer.class);

    private static SchemaRegistry registry = new SchemaRegistry();

    @Override
    public T deserialize(String topic, byte[] data) {
        return deserialize(data);
    }

    public T deserialize(byte[] message) {
        try {
            if (message == null) return null;
            byte magic = message[0];
            logger.trace("Message size: {}", message.length);
            Schema schema = registry.getSchema(magic);
            logger.trace("Deserializing from {} ({})", schema, magic);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(Arrays.copyOfRange(message, 1, message.length), null);
            DatumReader<T> reader = new SpecificDatumReader<T>(schema);
            T value = reader.read(null, decoder);
            return (T) value;
        } catch (Exception e) {
            logger.error("Error when deserializing byte[] to AVRO Specific", e);
            return null;
        }
    }
}

