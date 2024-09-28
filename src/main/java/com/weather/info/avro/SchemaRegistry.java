package com.weather.info.avro;

import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class SchemaRegistry implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SchemaRegistry.class);
    private static final String REGISTRY_FILE = "/avro-config/schema-registry.yaml";
    private Map<Integer, SchemaElement> schemas = new HashMap<>();

    public SchemaRegistry() {
        populateSchemas();
    }
    public String asString() {
        return schemas.toString();
    }
    public int getIndexForSchema(Schema schema) {
        return
                schemas.values()
                        .stream()
                        .filter(s -> schema.equals(s.getSchema()))
                        .findFirst()
                        .map(ele -> ele.getId())
                        .orElse(-1);
    }
    public Schema getSchemaFroMagic(int index) {
        if (!schemas.containsKey(index)) {
            logger.warn(
                    "Schema registry don't have info for index: {},  Registry:\n{}",
                    index,
                    schemas);
            schemas
                    .forEach((key, value) -> logger.warn(
                            "Registry index: {}, value: {}", key, value.getClazz()));
        }
        return schemas.get(index).getSchema();
    }
    private void populateSchemas() {
        schemas.clear();
        readYaml()
                .forEach(
                        s -> {
                            try {
                                Class<?> clazz = this.getClass().getClassLoader().loadClass(s.getClazz());
                                Method getSchema = clazz.getMethod("getClassSchema");
                                Schema schema = (Schema) getSchema.invoke(null);
                                s.setSchema(schema);
                                schemas.put(s.getId(), s);
                            } catch (Exception e) {
                                throw new RuntimeException("Unable to start.", e);
                            }
                        });
    }
    private List<SchemaElement> readYaml() {
        Yaml yaml = new Yaml(new Constructor(SchemaElement.class, new LoaderOptions()));
        InputStream stream = SchemaRegistry.class.getResourceAsStream(REGISTRY_FILE);
        List<SchemaElement> list = new ArrayList<>();
        yaml.loadAll(stream)
                .iterator()
                .forEachRemaining(
                        s -> {
                            SchemaElement scEl = (SchemaElement) s;
                            scEl.setId(list.size());
                            list.add(scEl);
                        });
        return list;
    }
}
