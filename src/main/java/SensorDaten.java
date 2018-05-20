import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Random;

public class SensorDaten implements Serializable {

  private String dateTime;
  private float airTemp;
  private float stdAirTemp;
  private float humidity;
  private float stdHumidity;
  private float irTemp;
  private float stdIrTemp;
  private float airPressure;
  private float stdAirPressure;
  private float windSpeed;
  private float stdWindSpeed;
  private float lightA;
  private float stdLightA;
  private float lightB;
  private float stdLightB;
  private float distance;
  private float stdDistance;
  private float counter;
  private float roll;
  private float pitch;
  private float xAccel;
  private float stdXAccel;
  private float yAccel;
  private float stdYAccel;
  private float zAccel;
  private float stdZAccel;
  private float battery;
  private float error;
  private float wdtTrace;
  private String crc3;

  public SensorDaten() {
    random = new Random();
    createDateTime();
    createAirTemp();
    createHumidity();
    createIrTemp();
    createAirPressure();
    createWindSpeed();
    createLight();
    createDistance();
    createCounter();
    createRoll();
    createPitch();
    createAccel();
    createBattery();
    createError();
    createWdtTrace();
    setCrc3("CRC ok");
  }

  private Random random;

  public String getDateTime() {
    return dateTime;
  }

  public void createDateTime() {
    setDateTime(LocalDateTime.now().toString());
  }

  public void setDateTime(String dateTime) {
    this.dateTime = dateTime;
  }

  public float getAirTemp() {
    return airTemp;
  }

  public void createAirTemp() {
    int min = -30;
    int max = 40;
    setAirTemp(min + random.nextFloat() * (max - min));
    setStdAirTemp(getAirTemp()/2);
  }

  public void setAirTemp(float airTemp) {
    this.airTemp = airTemp;
  }

  public float getStdAirTemp() {
    return stdAirTemp;
  }

  public void setStdAirTemp(float stdAirTemp) {
    this.stdAirTemp = stdAirTemp;
  }

  public float getHumidity() {
    return humidity;
  }

  public void createHumidity() {
    int min = 10;
    int max = 100;
    setHumidity(min + random.nextFloat() * (max - min));
    setStdHumidity(getHumidity()/2);
  }

  public void setHumidity(float humidity) {
    this.humidity = humidity;
  }

  public float getStdHumidity() {
    return stdHumidity;
  }

  public void setStdHumidity(float stdHumidity) {
    this.stdHumidity = stdHumidity;
  }

  public float getIrTemp() {
    return irTemp;
  }

  public void createIrTemp() {
    int min = 00;
    int max = 50;
    setIrTemp(min + random.nextFloat() * (max - min));
    setStdIrTemp(getIrTemp()/2);
  }

  public void setIrTemp(float irTemp) {
    this.irTemp = irTemp;
  }

  public float getStdIrTemp() {
    return stdIrTemp;
  }

  public void setStdIrTemp(float stdIrTemp) {
    this.stdIrTemp = stdIrTemp;
  }

  public float getAirPressure() {
    return airPressure;
  }

  public void createAirPressure() {
    int min = 800;
    int max = 1000;
    setAirPressure(min + random.nextFloat() * (max - min));
    setStdAirPressure(getAirPressure()/2);
  }

  public void setAirPressure(float airPressure) {
    this.airPressure = airPressure;
  }

  public float getStdAirPressure() {
    return stdAirPressure;
  }

  public void setStdAirPressure(float stdAirPressure) {
    this.stdAirPressure = stdAirPressure;
  }

  public float getWindSpeed() {
    return windSpeed;
  }

  public void createWindSpeed() {
    int min = 0;
    int max = 120;
    setWindSpeed(min + random.nextFloat() * (max - min));
    setStdWindSpeed(getWindSpeed()/2);
  }

  public void setWindSpeed(float windSpeed) {
    this.windSpeed = windSpeed;
  }

  public float getStdWindSpeed() {
    return stdWindSpeed;
  }

  public void setStdWindSpeed(float stdWindSpeed) {
    this.stdWindSpeed = stdWindSpeed;
  }

  public float getLightA() {
    return lightA;
  }

  public void setLightA(float lightA) {
    this.lightA = lightA;
  }

  public float getStdLightA() {
    return stdLightA;
  }

  public void setStdLightA(float stdLightA) {
    this.stdLightA = stdLightA;
  }

  public float getLightB() {
    return lightB;
  }

  public void setLightB(float lightB) {
    this.lightB = lightB;
  }

  public float getStdLightB() {
    return stdLightB;
  }

  public void setStdLightB(float stdLightB) {
    this.stdLightB = stdLightB;
  }

  public void createLight() {
    float min = 0.001f;
    float max = 0.005f;
    setLightA(min + random.nextFloat() * (max - min));
    setLightB(min + random.nextFloat() * (max - min));
    setStdLightA(getLightA()/2);
    setStdLightB(getLightB()/2);
  }

  public float getDistance() {
    return distance;
  }

  public void createDistance() {
    int min = 0;
    int max = 1;
    setDistance(min + random.nextFloat() * (max - min));
    setStdDistance(getDistance()/2);
  }

  public void setDistance(float distance) {
    this.distance = distance;
  }

  public float getStdDistance() {
    return stdDistance;
  }

  public void setStdDistance(float stdDistance) {
    this.stdDistance = stdDistance;
  }

  public float getCounter() {
    return counter;
  }

  public void createCounter() {
    int min = 0;
    int max = 100;
    setCounter(min + random.nextFloat() * (max - min));
  }

  public void setCounter(float counter) {
    this.counter = counter;
  }

  public float getRoll() {
    return roll;
  }

  public void createRoll() {
    int min = -100;
    int max = 100;
    setRoll(min + random.nextFloat() * (max - min));
  }

  public void setRoll(float roll) {
    this.roll = roll;
  }

  public float getPitch() {
    return pitch;
  }

  public void createPitch() {
    int min = -50;
    int max = 50;
    setPitch(min + random.nextFloat() * (max - min));
  }

  public void setPitch(float pitch) {
    this.pitch = pitch;
  }

  public float getxAccel() {
    return xAccel;
  }

  public void setxAccel(float xAccel) {
    this.xAccel = xAccel;
  }

  public float getStdXAccel() {
    return stdXAccel;
  }

  public void setStdXAccel(float stdXAccel) {
    this.stdXAccel = stdXAccel;
  }

  public float getyAccel() {
    return yAccel;
  }

  public void setyAccel(float yAccel) {
    this.yAccel = yAccel;
  }

  public float getStdYAccel() {
    return stdYAccel;
  }

  public void setStdYAccel(float stdYAccel) {
    this.stdYAccel = stdYAccel;
  }

  public float getzAccel() {
    return zAccel;
  }

  public void setzAccel(float zAccel) {
    this.zAccel = zAccel;
  }

  public float getStdZAccel() {
    return stdZAccel;
  }

  public void setStdZAccel(float stdZAccel) {
    this.stdZAccel = stdZAccel;
  }

  public void createAccel() {
    int min = -1;
    int max = 1;
    setxAccel(min + random.nextFloat() * (max - min));
    setyAccel(min + random.nextFloat() * (max - min));
    setzAccel(min + random.nextFloat() * (max - min));
    setStdXAccel(getxAccel()/2);
    setStdYAccel(getyAccel()/2);
    setStdZAccel(getzAccel()/2);
  }

  public float getBattery() {
    return battery;
  }

  public void createBattery() {
    int min = 0;
    int max = 10;
    setBattery(min + random.nextFloat() * (max - min));
  }

  public void setBattery(float battery) {
    this.battery = battery;
  }

  public float getError() {
    return error;
  }

  public void createError() {
    int min = 0;
    int max = 5;
    setError(min + random.nextFloat() * (max - min));
  }

  public void setError(float error) {
    this.error = error;
  }

  public float getWdtTrace() {
    return wdtTrace;
  }

  public void createWdtTrace() {
    int min = 0;
    int max = 1;
    setWdtTrace(min + random.nextFloat() * (max - min));
  }

  public void setWdtTrace(float wdtTrace) {
    this.wdtTrace = wdtTrace;
  }

  public String getCrc3() {
    return crc3;
  }

  public void setCrc3(String crc3) {
    this.crc3 = crc3;
  }

  @Override
  public String toString() {
    return "SensorDaten{"
            + "dateTime='"
            + dateTime
            + '\''
            + ", airTemp="
            + airTemp
            + ", stdAirTemp="
            + stdAirTemp
            + ", humidity="
            + humidity
            + ", stdHumidity="
            + stdHumidity
            + ", irTemp="
            + irTemp
            + ", stdIrTemp="
            + stdIrTemp
            + ", airPressure="
            + airPressure
            + ", stdAirPressure="
            + stdAirPressure
            + ", windSpeed="
            + windSpeed
            + ", stdWindSpeed="
            + stdWindSpeed
            + ", lightA="
            + lightA
            + ", stdLightA="
            + stdLightA
            + ", lightB="
            + lightB
            + ", stdLightB="
            + stdLightB
            + ", distance="
            + distance
            + ", stdDistance="
            + stdDistance
            + ", counter="
            + counter
            + ", roll="
            + roll
            + ", pitch="
            + pitch
            + ", xAccel="
            + xAccel
            + ", stdXAccel="
            + stdXAccel
            + ", yAccel="
            + yAccel
            + ", stdYAccel="
            + stdYAccel
            + ", zAccel="
            + zAccel
            + ", stdZAccel="
            + stdZAccel
            + ", battery="
            + battery
            + ", error="
            + error
            + ", wdtTrace="
            + wdtTrace
            + ", crc3='"
            + crc3
            + '\''
            + '}';
  }
}



/*
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Random;

public class SensorDaten implements Serializable {

  private String dateTime;
  private float airTemp;
  private float stdAirTemp;
  private float humidity;
  private float stdHumidity;
  private float irTemp;
  private float stdIrTemp;
  private float airPressure;
  private float stdAirPressure;
  private float windSpeed;
  private float stdWindSpeed;
  private float lightA;
  private float stdLightA;
  private float lightB;
  private float stdLightB;
  private float distance;
  private float stdDistance;
  private float counter;
  private float roll;
  private float pitch;
  private float xAccel;
  private float stdXAccel;
  private float yAccel;
  private float stdYAccel;
  private float zAccel;
  private float stdZAccel;
  private float battery;
  private float error;
  private float wdtTrace;
  private String crc3;

  public SensorDaten() {
    random = new Random();
    createDateTime();
    createAirTemp();
    createHumidity();
    createIrTemp();
    createAirPressure();
    createWindSpeed();
    createLight();
    createDistance();
    createCounter();
    createRoll();
    createPitch();
    createAccel();
    createBattery();
    createError();
    createWdtTrace();
    setCrc3("CRC ok");
  }

  private Random random;

  public String getDateTime() {
    return dateTime;
  }

  public void createDateTime() {
    setDateTime(LocalDateTime.now().toString());
  }

  public void setDateTime(String dateTime) {
    this.dateTime = dateTime;
  }

  public float getAirTemp() {
    return airTemp;
  }

  public void createAirTemp() {
    int min = -30;
    int max = 40;
    setAirTemp(min + random.nextFloat() * (max - min));
  }

  public void setAirTemp(float airTemp) {
    this.airTemp = airTemp;
  }

  public float getStdAirTemp() {
    return stdAirTemp;
  }

  public void setStdAirTemp(float stdAirTemp) {
    this.stdAirTemp = stdAirTemp;
  }

  public float getHumidity() {
    return humidity;
  }

  public void createHumidity() {
    int min = 10;
    int max = 100;
    setHumidity(min + random.nextFloat() * (max - min));
  }

  public void setHumidity(float humidity) {
    this.humidity = humidity;
  }

  public float getStdHumidity() {
    return stdHumidity;
  }

  public void setStdHumidity(float stdHumidity) {
    this.stdHumidity = stdHumidity;
  }

  public float getIrTemp() {
    return irTemp;
  }

  public void createIrTemp() {
    int min = 00;
    int max = 50;
    setIrTemp(min + random.nextFloat() * (max - min));
  }

  public void setIrTemp(float irTemp) {
    this.irTemp = irTemp;
  }

  public float getStdIrTemp() {
    return stdIrTemp;
  }

  public void setStdIrTemp(float stdIrTemp) {
    this.stdIrTemp = stdIrTemp;
  }

  public float getAirPressure() {
    return airPressure;
  }

  public void createAirPressure() {
    int min = 800;
    int max = 1000;
    setAirPressure(min + random.nextFloat() * (max - min));
  }

  public void setAirPressure(float airPressure) {
    this.airPressure = airPressure;
  }

  public float getStdAirPressure() {
    return stdAirPressure;
  }

  public void setStdAirPressure(float stdAirPressure) {
    this.stdAirPressure = stdAirPressure;
  }

  public float getWindSpeed() {
    return windSpeed;
  }

  public void createWindSpeed() {
    int min = 0;
    int max = 120;
    setWindSpeed(min + random.nextFloat() * (max - min));
  }

  public void setWindSpeed(float windSpeed) {
    this.windSpeed = windSpeed;
  }

  public float getStdWindSpeed() {
    return stdWindSpeed;
  }

  public void setStdWindSpeed(float stdWindSpeed) {
    this.stdWindSpeed = stdWindSpeed;
  }

  public float getLightA() {
    return lightA;
  }

  public void setLightA(float lightA) {
    this.lightA = lightA;
  }

  public float getStdLightA() {
    return stdLightA;
  }

  public void setStdLightA(float stdLightA) {
    this.stdLightA = stdLightA;
  }

  public float getLightB() {
    return lightB;
  }

  public void setLightB(float lightB) {
    this.lightB = lightB;
  }

  public float getStdLightB() {
    return stdLightB;
  }

  public void setStdLightB(float stdLightB) {
    this.stdLightB = stdLightB;
  }

  public void createLight() {
    float min = 0.001f;
    float max = 0.005f;
    setLightA(min + random.nextFloat() * (max - min));
    setLightB(min + random.nextFloat() * (max - min));
  }

  public float getDistance() {
    return distance;
  }

  public void createDistance() {
    int min = 0;
    int max = 1;
    setDistance(min + random.nextFloat() * (max - min));
  }

  public void setDistance(float distance) {
    this.distance = distance;
  }

  public float getStdDistance() {
    return stdDistance;
  }

  public void setStdDistance(float stdDistance) {
    this.stdDistance = stdDistance;
  }

  public float getCounter() {
    return counter;
  }

  public void createCounter() {
    int min = 0;
    int max = 100;
    setCounter(min + random.nextFloat() * (max - min));
  }

  public void setCounter(float counter) {
    this.counter = counter;
  }

  public float getRoll() {
    return roll;
  }

  public void createRoll() {
    int min = -100;
    int max = 100;
    setRoll(min + random.nextFloat() * (max - min));
  }

  public void setRoll(float roll) {
    this.roll = roll;
  }

  public float getPitch() {
    return pitch;
  }

  public void createPitch() {
    int min = -50;
    int max = 50;
    setPitch(min + random.nextFloat() * (max - min));
  }

  public void setPitch(float pitch) {
    this.pitch = pitch;
  }

  public float getxAccel() {
    return xAccel;
  }

  public void setxAccel(float xAccel) {
    this.xAccel = xAccel;
  }

  public float getStdXAccel() {
    return stdXAccel;
  }

  public void setStdXAccel(float stdXAccel) {
    this.stdXAccel = stdXAccel;
  }

  public float getyAccel() {
    return yAccel;
  }

  public void setyAccel(float yAccel) {
    this.yAccel = yAccel;
  }

  public float getStdYAccel() {
    return stdYAccel;
  }

  public void setStdYAccel(float stdYAccel) {
    this.stdYAccel = stdYAccel;
  }

  public float getzAccel() {
    return zAccel;
  }

  public void setzAccel(float zAccel) {
    this.zAccel = zAccel;
  }

  public float getStdZAccel() {
    return stdZAccel;
  }

  public void setStdZAccel(float stdZAccel) {
    this.stdZAccel = stdZAccel;
  }

  public void createAccel() {
    int min = -1;
    int max = 1;
    setxAccel(min + random.nextFloat() * (max - min));
    setyAccel(min + random.nextFloat() * (max - min));
    setzAccel(min + random.nextFloat() * (max - min));
  }

  public float getBattery() {
    return battery;
  }

  public void createBattery() {
    int min = 0;
    int max = 10;
    setBattery(min + random.nextFloat() * (max - min));
  }

  public void setBattery(float battery) {
    this.battery = battery;
  }

  public float getError() {
    return error;
  }

  public void createError() {
    int min = 0;
    int max = 5;
    setError(min + random.nextFloat() * (max - min));
  }

  public void setError(float error) {
    this.error = error;
  }

  public float getWdtTrace() {
    return wdtTrace;
  }

  public void createWdtTrace() {
    int min = 0;
    int max = 1;
    setWdtTrace(min + random.nextFloat() * (max - min));
  }

  public void setWdtTrace(float wdtTrace) {
    this.wdtTrace = wdtTrace;
  }

  public String getCrc3() {
    return crc3;
  }

  public void setCrc3(String crc3) {
    this.crc3 = crc3;
  }

  @Override
  public String toString() {
    return "SensorDaten{"
        + "dateTime='"
        + dateTime
        + '\''
        + ", airTemp="
        + airTemp
        + ", stdAirTemp="
        + stdAirTemp
        + ", humidity="
        + humidity
        + ", stdHumidity="
        + stdHumidity
        + ", irTemp="
        + irTemp
        + ", stdIrTemp="
        + stdIrTemp
        + ", airPressure="
        + airPressure
        + ", stdAirPressure="
        + stdAirPressure
        + ", windSpeed="
        + windSpeed
        + ", stdWindSpeed="
        + stdWindSpeed
        + ", lightA="
        + lightA
        + ", stdLightA="
        + stdLightA
        + ", lightB="
        + lightB
        + ", stdLightB="
        + stdLightB
        + ", distance="
        + distance
        + ", stdDistance="
        + stdDistance
        + ", counter="
        + counter
        + ", roll="
        + roll
        + ", pitch="
        + pitch
        + ", xAccel="
        + xAccel
        + ", stdXAccel="
        + stdXAccel
        + ", yAccel="
        + yAccel
        + ", stdYAccel="
        + stdYAccel
        + ", zAccel="
        + zAccel
        + ", stdZAccel="
        + stdZAccel
        + ", battery="
        + battery
        + ", error="
        + error
        + ", wdtTrace="
        + wdtTrace
        + ", crc3='"
        + crc3
        + '\''
        + '}';
  }
}

 */