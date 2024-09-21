package com.weather.info.avro;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class WeatherPayloadTest {

    @Test
    void testCameraDemographicsPayload() {
        WeatherData weatherData = WeatherData.newBuilder()
                .setDeviceId("123")
                .setTimestamp(23123)
                .setWind(Wind.newBuilder()
                        .setDegree(1)
                        .setUnit("123")
                        .setDirection("321")
                        .setGustSpeed(312D)
                        .setSpeed(123)
                        .build())
                .setTemperature(Temperature.newBuilder()
                        .setCurrent(321)
                        .setUnit("123")
                        .build())
                .setPressure(Pressure.newBuilder()
                        .setValue(123)
                        .setUnit("345")
                        .build())
                .setLocation(Location.newBuilder()
                        .setLongitude(3212)
                        .setLatitude(23213)
                        .build())
                .setHumidity(Humidity.newBuilder()
                        .setUnit("213")
                        .setValue(2135)
                        .build())
                .build();

        try (AvroDeserializer<GenericRecord> deser = new AvroDeserializer<>();
             AvroSerializer ser = new AvroSerializer()) {
            byte[] bytes = ser.serialize("test", weatherData);
            GenericRecord obj = deser.deserialize("test", bytes);
            System.out.println("Weather Data - deserialsed : " + obj);
        }
    }
}
