package tech.meliora.academy.streamsexample.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class HashMapDeserializer implements Deserializer<Map> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public Map deserialize(String s, byte[] bytes) {
        ObjectMapper objectMapper = new ObjectMapper();

        Map map = null;

        try {
            map = objectMapper.readValue(bytes, Map.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return map;
    }

    @Override
    public void close() {

    }



   /* @Override
    public Map deserialize(InputStream arg1) {
        ObjectMapper objectMapper = new ObjectMapper();

        Map map = null;
        try {
            map = objectMapper.readValue(arg1, Map.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;

    }*/
}
