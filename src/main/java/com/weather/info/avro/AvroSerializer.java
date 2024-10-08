package com.weather.info.avro;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
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
    private static final long serialVersionUID = -2295385122095243161L;
    static Logger logger = LoggerFactory.getLogger(AvroSerializer.class);
    private static final SchemaRegistry registry = new SchemaRegistry();
    @Override
    public byte[] serialize(String topic, GenericRecord data) {

        try {
            if (data == null) return new byte[0];
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            int index = registry.getIndexForSchema(data.getSchema());
            logger.trace(" Serializing from {} ({})", data.getSchema(), index);
            out.write(index);
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(data.getSchema());
            writer.write(data, encoder);
            encoder.flush();
            out.close();
            return out.toByteArray();
        } catch (Exception e) {
            throw new SerializationException("Error when serializing Avro record to byte[] ", e);
        }
    }
}
