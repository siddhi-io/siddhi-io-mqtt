package org.wso2.extension.siddhi.io.mqtt.sink;

import io.moquette.server.Server;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.rmi.RemoteException;
import java.util.Properties;

public class MqttPublishTestCase {
    private volatile int count;
    private volatile boolean eventArrived;
    private static final Logger log = Logger.getLogger(MqttSinkTestCase.class);
    private static final Server mqttBroker = new Server();
    private MqttTestClient mqttTestClient;

    @BeforeMethod
    public void initBeforeMethod() throws Exception {
        count = 0;
        eventArrived = false;
        try {
            Properties properties = new Properties();
            properties.put("port", Integer.toString(1883));
            properties.put("host", "0.0.0.0");
            final IConfig config = new MemoryConfig(properties);
            mqttBroker.startServer(config);
            Thread.sleep(1000);
        } catch (Exception e) {
            throw new RemoteException("Exception caught when starting server", e);
        }
    }

    @Test
    public void mqttPublishTest() {
        log.info("Test for publishing events when broker is stopped");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (symbol string, price float, volume long); " + "@info(name = 'query1') "
                        + "@sink(type='mqtt', url= 'tcp://localhost:1883', "
                        + "topic='mqtt_publish_event',username='mqtt-user', "
                        + "password='mqtt-password', clean.session='true', message.retain='false', "
                        + "quality.of.service= '1', keep.alive= '60'," + "@map(type='xml'))"
                        + "Define stream BarStream (symbol string, price float, volume long);"
                        + "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        try {
            this.mqttTestClient = new MqttTestClient("tcp://localhost:1883", "mqtt_publish_event", 1);
        } catch (ConnectionUnavailableException e) {
            AssertJUnit.fail("Could not connect to broker.");
        }
        siddhiAppRuntime.start();
        try {
            fooStream.send(new Object[] { "WSO2", 55.6f, 100L });
            fooStream.send(new Object[] { "IBM", 75.6f, 100L });
            fooStream.send(new Object[] { "WSO2", 57.6f, 100L });
            Thread.sleep(500);
            mqttBroker.stopServer();
            fooStream.send(new Object[] { "IBM", 75.6f, 100L });
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was  interrupted");
        }
        count = mqttTestClient.getCount();
        eventArrived = mqttTestClient.getEventArrived();
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}
