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
    private static final String REGISTRY_FILE = "/avro-config/schema-registry.yaml";
    private Map<Integer, SchemaElement> schemas = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(SchemaRegistry.class);
    public SchemaRegistry() {
        init();
    }
    public String asString() {
        return schemas.toString();
    }
    public int getIndex(Schema schema) {
        return
                schemas.values()
                        .stream()
                        .filter(s -> schema.equals(s.getSchema()))
                        .findFirst()
                        .map(SchemaElement::getId)
                        .orElse(-1);
    }
    public Schema getSchema(int index) {
        if (!schemas.containsKey(index)) {
            logger.warn(
                    "Schema registry doesn't have entry for index: {},  registry contents:{}",
                    index,
                    schemas);
            schemas
                    .forEach((key, value) -> logger.warn(
                            "Registry index: {}, value: {}", key, value.getClazz()));
        }
        return schemas.get(index).getSchema();
    }
    private void init() {
        schemas.clear();
        loadYaml()
                .forEach(
                        s -> {
                            try {
                                Class<?> clazz = this.getClass().getClassLoader().loadClass(s.getClazz());
                                Method getSchema = clazz.getMethod("getClassSchema");
                                Schema schema = (Schema) getSchema.invoke(null);
                                s.setSchema(schema);
                                schemas.put(s.getId(), s);
                            } catch (Exception e) {
                                throw new RuntimeException("Cannot start.", e);
                            }
                        });
    }
    private List<SchemaElement> loadYaml() {
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
