/*
 *  Copyright (c) 2017 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package io.siddhi.extension.io.mqtt.sink;

import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.input.InputHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.rmi.RemoteException;
import java.util.Properties;

public class MqttSinkMapTest {
    private volatile int count;
    private volatile boolean eventArrived;
    private static final Logger log = LogManager.getLogger(MqttSinkMapTest.class);
    private static final Server mqttBroker = new Server();
    private MqttTestClient mqttTestClient;

    @BeforeMethod
    public void initBeforeMethod() {
        count = 0;
        eventArrived = false;
    }

    @BeforeClass
    public static void init() throws Exception {
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

    @AfterClass
    public static void stop() {
        mqttBroker.stopServer();
    }

    @Test
    public void mqttPublishWithXmlMapping() {
        log.info("Mqtt Publish test for XML Mapping");
        SiddhiManager siddhiManager = new SiddhiManager();
        ResultContainer resultContainer = new ResultContainer(3);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type='mqtt', url= 'tcp://localhost:1883', " +
                        "topic='mqtt_publish_xml_map',username='mqtt-user', " +
                        "password='mqtt-password',keep.alive='60', message.retain='false', " +
                        "quality.of.service= '1', clean.session='true'," +
                        "@map(type='xml'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");

        try {
            this.mqttTestClient = new MqttTestClient("tcp://localhost:1883",
                    "mqtt_publish_xml_map", 1, resultContainer);
        } catch (ConnectionUnavailableException e) {
            AssertJUnit.fail("Could not connect to broker.");
        }
        siddhiAppRuntime.start();
        try {
            fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
            fooStream.send(new Object[]{"IBM", 75.6f, 100L});
            fooStream.send(new Object[]{"WSO2", 57.6f, 100L});
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was interrupted");
        }
        count = mqttTestClient.getCount();
        eventArrived = mqttTestClient.getEventArrived();
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("WSO2"));
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("IBM"));
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("WSO2"));
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void mqttPublishWithJsonMapping() {
        log.info("Mqtt Publish test for JSON Mapping");
        SiddhiManager siddhiManager = new SiddhiManager();
        ResultContainer resultContainer = new ResultContainer(3);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type='mqtt', url= 'tcp://localhost:1883', " +
                        "topic='mqtt_publish_json_map',username='mqtt-user', " +
                        "password='mqtt-password',keep.alive='60', message.retain='false', " +
                        "quality.of.service= '1', clean.session='true'," +
                        "@map(type='json'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        try {
            this.mqttTestClient = new MqttTestClient("tcp://localhost:1883",
                    "mqtt_publish_json_map", 1, resultContainer);
        } catch (ConnectionUnavailableException e) {
            AssertJUnit.fail("Could not connect to broker.");
        }
        siddhiAppRuntime.start();
        try {
            fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
            fooStream.send(new Object[]{"IBM", 75.6f, 100L});
            fooStream.send(new Object[]{"WSO2", 57.6f, 100L});
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was interrupted");
        }
        count = mqttTestClient.getCount();
        eventArrived = mqttTestClient.getEventArrived();
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("WSO2"));
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("IBM"));
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("WSO2"));
        siddhiAppRuntime.shutdown();
    }
}
