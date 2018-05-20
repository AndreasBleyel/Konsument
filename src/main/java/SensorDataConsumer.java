import com.sun.security.ntlm.Server;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.Collections;
import java.util.Properties;

public class SensorDataConsumer {

  public static void consume() {
    KafkaConsumer<Long, SensorDaten> consumer = createConsumer();

    try {
      consumer.subscribe(Collections.singletonList(ServerConfiguration.TOPIC));
      while (true) {
        ConsumerRecords<Long, SensorDaten> sensorDaten = consumer.poll(100);
        sensorDaten.forEach(
            datum -> {
              CassandraConnector.writeData(datum.key(), datum.value());
              System.out.printf(
                  "Consumer Record:(%d, %s, %d, %d)\n",
                  datum.key(), datum.value(), datum.partition(), datum.offset());
            });
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      consumer.close();
    }
  }

  private static KafkaConsumer<Long, SensorDaten> createConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ServerConfiguration.BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "SensorDatenConsumer");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorDatenDeserializer.class.getName());
    return new KafkaConsumer<Long, SensorDaten>(props);
  }
}
