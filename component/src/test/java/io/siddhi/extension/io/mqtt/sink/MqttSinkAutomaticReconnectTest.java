package io.siddhi.extension.io.mqtt.sink;

import io.moquette.server.Server;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.input.InputHandler;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.rmi.RemoteException;
import java.util.Properties;

public class MqttSinkAutomaticReconnectTest {
  private volatile int count;
  private volatile boolean eventArrived;
  private static final Logger log = Logger.getLogger(MqttSinkAutomaticReconnectTest.class);
  private static final Server mqttBroker = new Server();
  private MqttTestClient mqttTestClient;
  private static final Properties properties = new Properties();

  @BeforeMethod
  public void initBeforeMethod() {
    count = 0;
    eventArrived = false;
  }

  @BeforeClass
  public static void init() throws Exception {
    try {
      properties.put("port", Integer.toString(1883));
      properties.put("host", "0.0.0.0");
      final IConfig config = new MemoryConfig(properties);
      mqttBroker.startServer(config);
      Thread.sleep(1000);
    } catch (Exception e) {
      throw new RemoteException("Exception caught when starting server", e);
    }
  }

  @AfterClass
  public static void stop() {
    mqttBroker.stopServer();
  }

  @Test
  public void mqttPublishEventWithAutomaticReconnect() {
    log.info("Test for Mqtt publish events with automatic reconnect enabled");
    SiddhiManager siddhiManager = new SiddhiManager();
    ResultContainer resultContainer = new ResultContainer(3);
    SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
        "define stream FooStream (symbol string, price float, volume long); " + "@info(name = 'query1') "
            + "@sink(type='mqtt', url= 'tcp://localhost:1883', "
            + "topic='mqtt_publish_event_with_automatic_reconnect',username='mqtt-user', "
            + "password='mqtt-password', clean.session='true', message.retain='false', "
            + "automatic.reconnect='true', keep.alive= '60'," + "@map(type='xml'))"
            + "Define stream BarStream (symbol string, price float, volume long);"
            + "from FooStream select symbol, price, volume insert into BarStream;");
    InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
    try {
      this.mqttTestClient = new MqttTestClient("tcp://localhost:1883",
          "mqtt_publish_event_with_automatic_reconnect", 1,
          resultContainer, true, false);
    } catch (ConnectionUnavailableException e) {
      AssertJUnit.fail("Could not connect to broker.");
    }
    siddhiAppRuntime.start();
    try {
      fooStream.send(new Object[] { "WSO2", 55.6f, 100L });
      fooStream.send(new Object[] { "IBM", 75.6f, 100L });
      Thread.sleep(500);
      mqttBroker.stopServer();
      fooStream.send(new Object[] { "WSO2", 57.6f, 100L });
    } catch (InterruptedException e) {
      AssertJUnit.fail("Thread sleep was interrupted");
    }
    try {
      final IConfig config = new MemoryConfig(properties);
      mqttBroker.startServer(config);
      Thread.sleep(2000);
      fooStream.send(new Object[] { "JAMES", 58.6f, 100L });
      Thread.sleep(500);
    } catch (Exception e) {
      AssertJUnit.fail("Thread sleep was interrupted");
    }
    count = mqttTestClient.getCount();
    eventArrived = mqttTestClient.getEventArrived();
    AssertJUnit.assertEquals(3, count);
    AssertJUnit.assertTrue(eventArrived);
    AssertJUnit.assertTrue(resultContainer.assertMessageContent("WSO2"));
    AssertJUnit.assertTrue(resultContainer.assertMessageContent("IBM"));
    AssertJUnit.assertTrue(resultContainer.assertMessageContent("JAMES"));
    siddhiAppRuntime.shutdown();
  }

  @Test
  public void mqttPublishEventWithoutAutomaticReconnect() {
    log.info("Test for Mqtt publish events with automatic reconnect disabled");
    SiddhiManager siddhiManager = new SiddhiManager();
    ResultContainer resultContainer = new ResultContainer(2);
    SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
        "define stream FooStream (symbol string, price float, volume long); " + "@info(name = 'query1') "
            + "@sink(type='mqtt', url= 'tcp://localhost:1883', "
            + "topic='mqtt_publish_event_without_automatic_reconnect',username='mqtt-user', "
            + "password='mqtt-password', clean.session='true', message.retain='false', "
            + "automatic.reconnect='false', keep.alive= '60'," + "@map(type='xml'))"
            + "Define stream BarStream (symbol string, price float, volume long);"
            + "from FooStream select symbol, price, volume insert into BarStream;");
    InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
    try {
      this.mqttTestClient = new MqttTestClient("tcp://localhost:1883",
          "mqtt_publish_event_without_automatic_reconnect", 1, resultContainer,
          true, false);
    } catch (ConnectionUnavailableException e) {
      AssertJUnit.fail("Could not connect to broker.");
    }
    siddhiAppRuntime.start();
    try {
      fooStream.send(new Object[] { "WSO2", 55.6f, 100L });
      fooStream.send(new Object[] { "IBM", 75.6f, 100L });
      Thread.sleep(500);
      mqttBroker.stopServer();
      fooStream.send(new Object[] { "WSO2", 57.6f, 100L });
    } catch (InterruptedException e) {
      AssertJUnit.fail("Thread sleep was interrupted");
    }
    try {
      final IConfig config = new MemoryConfig(properties);
      mqttBroker.startServer(config);
      Thread.sleep(2000);
      fooStream.send(new Object[] { "JAMES", 58.6f, 100L });
      Thread.sleep(500);
    } catch (Exception e) {
      AssertJUnit.fail("Thread sleep was interrupted");
    }

    count = mqttTestClient.getCount();
    eventArrived = mqttTestClient.getEventArrived();
    AssertJUnit.assertEquals(2, count);
    AssertJUnit.assertTrue(eventArrived);
    AssertJUnit.assertTrue(resultContainer.assertMessageContent("WSO2"));
    AssertJUnit.assertTrue(resultContainer.assertMessageContent("IBM"));
    siddhiAppRuntime.shutdown();
  }
}
