package com.weather.info.avro;

import java.io.Serializable;
import java.util.Arrays;
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
    private static final SchemaRegistry registry = new SchemaRegistry();
    @Override
    public T deserialize(String topic, byte[] data) {
        return deserialize(data);
    }
    public T deserialize(byte[] message) {
        try {
            if (message == null) return null;
            byte magic = message[0];
            Schema schema = registry.getSchemaFroMagic(magic);
            logger.trace("Message size: {}, Deserializing from {} ({})", message.length, schema, magic);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(Arrays.copyOfRange(message, 1, message.length), null);
            DatumReader<T> reader = new SpecificDatumReader<>(schema);
            return reader.read(null, decoder);
        } catch (Exception e) {
            logger.error("Error when deserializing byte[] to AVRO Specific", e);
            return null;
        }
    }
}

