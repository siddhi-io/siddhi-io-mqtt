package io.siddhi.extension.io.mqtt.source;

import io.moquette.server.Server;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.rmi.RemoteException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class MqttSourceAutomaticReconnectTest {
  static final Logger LOG = Logger.getLogger(MqttSourceTestCase.class);
  private AtomicInteger count = new AtomicInteger(0);
  private int waitTime = 50;
  private int timeout = 60000;
  private volatile boolean eventArrived;
  private static final Server mqttBroker = new Server();
  private static final Properties properties = new Properties();

  @BeforeMethod
  public void initBeforeMethod() {
    count.set(0);
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
  public void mqttReceiveEventsWithAutomaticReconnect() {
    LOG.info("Test for receiving events with automatic reconnect enabled");
    SiddhiManager siddhiManager = new SiddhiManager();
    SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
        "@App:name('TestExecutionPlan2') "
            + "define stream BarStream2 (symbol string, price float, volume long); "
            + "@info(name = 'query1') "
            + "@source(type='mqtt',url= 'tcp://localhost:1883',topic = 'mqtt_receive_event_with_automatic_reconnect',"
            + "automatic.reconnect='true', clean.session='false', keep.alive= '60',@map(type='xml'))"
            + "Define stream FooStream2 (symbol string, price float, volume long);"
            + "from FooStream2 select symbol, price, volume insert into BarStream2;");
    siddhiAppRuntimeSource.addCallback("BarStream2", new StreamCallback() {
      @Override
      public void receive(Event[] events) {
        for (Event event : events) {
          LOG.info(event);
          eventArrived = true;
          count.incrementAndGet();
        }
      }
    });
    siddhiAppRuntimeSource.start();
    try {
      Thread.sleep(4000);
    } catch (InterruptedException e) {
      AssertJUnit.fail("Thread sleep was interrupted");
    }
    SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime("@App:name('TestExecutionPlan') "
        + "define stream FooStream (symbol string, price float, volume long); " + "@info(name = 'query1') "
        + "@sink(type='mqtt', url= 'tcp://localhost:1883', "
        + "topic='mqtt_receive_event_with_automatic_reconnect', clean.session='true', message.retain='false', "
        + "automatic.reconnect='true', quality.of.service= '1',keep.alive= '60'," + "@map(type='xml'))"
        + "Define stream BarStream (symbol string, price float, volume long);"
        + "from FooStream select symbol, price, volume insert into BarStream;");
    InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
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
      SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
    } catch (Exception e) {
      AssertJUnit.fail("Thread sleep was interrupted");
    }
    siddhiAppRuntimeSource.shutdown();
    siddhiAppRuntime.shutdown();

  }

  @Test
  public void mqttReceiveEventsWithoutAutomaticReconnect() {
    LOG.info("Test for receiving events with automatic reconnect disabled");
    SiddhiManager siddhiManager = new SiddhiManager();
    SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
        "@App:name('TestExecutionPlan2') "
            + "define stream BarStream2 (symbol string, price float, volume long); "
            + "@info(name = 'query1') "
            + "@source(type='mqtt',url= 'tcp://localhost:1883',topic='mqtt_receive_event_without_automatic_reconnect',"
            + "automatic.reconnect='false', clean.session='true', keep.alive= '60',@map(type='xml'))"
            + "Define stream FooStream2 (symbol string, price float, volume long);"
            + "from FooStream2 select symbol, price, volume insert into BarStream2;");
    siddhiAppRuntimeSource.addCallback("BarStream2", new StreamCallback() {
      @Override
      public void receive(Event[] events) {
        for (Event event : events) {
          LOG.info(event);
          eventArrived = true;
          count.incrementAndGet();
        }
      }
    });
    siddhiAppRuntimeSource.start();
    try {
      Thread.sleep(4000);
    } catch (InterruptedException e) {
      AssertJUnit.fail("Thread sleep was interrupted");
    }
    SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime("@App:name('TestExecutionPlan') "
        + "define stream FooStream (symbol string, price float, volume long); " + "@info(name = 'query1') "
        + "@sink(type='mqtt', url= 'tcp://localhost:1883', "
        + "topic='mqtt_receive_event_without_automatic_reconnect', clean.session='true', message.retain='false', "
        + "automatic.reconnect='true', quality.of.service= '1',keep.alive= '60'," + "@map(type='xml'))"
        + "Define stream BarStream (symbol string, price float, volume long);"
        + "from FooStream select symbol, price, volume insert into BarStream;");
    InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
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
      SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
    } catch (Exception e) {
      AssertJUnit.fail("Thread sleep was interrupted");
    }
    siddhiAppRuntimeSource.shutdown();
    siddhiAppRuntime.shutdown();

  }
}
