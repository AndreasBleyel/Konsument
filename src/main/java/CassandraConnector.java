import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import static com.datastax.driver.core.querybuilder.QueryBuilder.now;

public class CassandraConnector {

  private static final String KEYSPACE = "ba2";
  private static final String SERVER_IP = "127.0.0.1";
  private static Cluster cluster;
  private static Session session;

  public static void connect() {

    cluster = Cluster.builder().addContactPoint(SERVER_IP).build();
    session = cluster.connect();
    session.execute("USE " + KEYSPACE);
  }

  public static void writeData(Long key, SensorDaten datum) {

    try {
      connect();

      session.execute(
          "INSERT INTO wetterdaten (id, date_time, air_temp, std_air_temp, humidity, std_humidity,"
              + "IR_temp, std_IR_temp, air_pressure, std_pressure, wind_speed, std_wind_speed, light_A,"
              + "std_light_A, light_B, std_light_B, distance, std_distance, counter, roll, pitch,"
              + "X_accel, std_X_accel, Y_accel, std_Y_accel, Z_accel, std_Z_accel, battery, error,"
              + "WDT_trace, crc3) "
              + "VALUES("
              + key
              + ",'"
              + datum.getDateTime()
              + "',"
              + datum.getAirTemp()
              + ","
              + datum.getStdAirTemp()
              + ","
              + datum.getHumidity()
              + ","
              + datum.getStdHumidity()
              + ","
              + datum.getIrTemp()
              + ","
              + datum.getStdIrTemp()
              + ","
              + datum.getAirPressure()
              + ","
              + datum.getStdAirPressure()
              + ","
              + datum.getWindSpeed()
              + ","
              + datum.getStdWindSpeed()
              + ","
              + datum.getLightA()
              + ","
              + datum.getStdLightA()
              + ","
              + datum.getLightB()
              + ","
              + datum.getStdLightB()
              + ","
              + datum.getDistance()
              + ","
              + datum.getStdDistance()
              + ","
              + datum.getCounter()
              + ","
              + datum.getRoll()
              + ","
              + datum.getPitch()
              + ","
              + datum.getxAccel()
              + ","
              + datum.getStdXAccel()
              + ","
              + datum.getyAccel()
              + ","
              + datum.getStdYAccel()
              + ","
              + datum.getzAccel()
              + ","
              + datum.getStdZAccel()
              + ","
              + datum.getBattery()
              + ","
              + datum.getError()
              + ","
              + datum.getWdtTrace()
              + ",'"
              + datum.getCrc3()
              + "');");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (cluster != null) cluster.close();
    }
  }
}

/*

 session.execute(
          "CREATE TABLE wetterdaten IF NOT EXISTS("
              + "id bigint primary key,"
              + "date_time text,"
              + "air_temp float,"
              + "std_air_temp float,"
              + "humidity float,"
              + "std_humidity float,"
              + "IR_temp float,"
              + "std_IR_temp float,"
              + "air_pressure float,"
              + "std_pressure float,"
              + "wind_speed float,"
              + "std_wind_speed float,"
              + "light_A float,"
              + "std_light_A float,"
              + "light_B float,"
              + "std_light_B float,"
              + "distance float,"
              + "std_distance float,"
              + "counter float,"
              + "roll float,"
              + "pitch float,"
              + "X_accel float,"
              + "std_X_accel float,"
              + "Y_accel float,"
              + "std_Y_accel float,"
              + "Z_accel float,"
              + "std_Z_accel float,"
              + "battery float,"
              + "error float,"
              + "WDT_trace float,"
              + "crc3 text"
              + ");");

session.execute(
          "INSERT INTO wetterdaten (id, date_time, air_temp, std_air_temp, humidity, std_humidity," +
                  "IR_temp, std_IR_temp, air_pressure, std_pressure, wind_speed, std_wind_speed, light_A," +
                  "std_light_A, light_B, std_light_B, distance, std_distance, counter, roll, pitch," +
                  "X_accel, std_X_accel, Y_accel, std_Y_accel, Z_accel, std_Z_accel, battery, error," +
                  "WDT_trace, crc3) " +
                  "VALUES("+now()+", );");

 */
