package com.weather.info.avro;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.Map;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroSerializer implements Serializer<GenericRecord>, Serializable {

    private static final long serialVersionUID = 55783815222597735L;

    static Logger LOG = LoggerFactory.getLogger(AvroSerializer.class);

    private static SchemaRegistry registry = new SchemaRegistry();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, GenericRecord data) {

        try {
            if (data == null) {
                return null;
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            int index = registry.getIndex(data.getSchema());
            LOG.trace(" Serializing from {} ({})", data.getSchema(), index);

            out.write(index);

            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(data.getSchema());
            writer.write(data, encoder);
            encoder.flush();
            out.close();
            byte[] serializedBytes = out.toByteArray();
            return serializedBytes;
        } catch (Exception e) {
            throw new SerializationException("Error when serializing Avro record to byte[] ", e);
        }
    }

    @Override
    public void close() {}
}
