import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class SensorDatenDeserializer implements Deserializer<SensorDaten>{

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public SensorDaten deserialize(String topic, byte[] data) {

        if(data == null)
            return null;

        try{
            return SerializationUtils.deserialize(data);
        }catch (RuntimeException e){
            throw new SerializationException("Error deserializing value",e);
        }
    }

    @Override
    public void close() {

    }
}
