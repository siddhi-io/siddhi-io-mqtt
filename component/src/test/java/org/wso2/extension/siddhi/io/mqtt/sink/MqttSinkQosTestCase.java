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
package org.wso2.extension.siddhi.io.mqtt.sink;

import io.moquette.server.Server;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.rmi.RemoteException;
import java.util.Properties;

public class MqttSinkQosTestCase {
    private volatile int count;
    private volatile boolean eventArrived;
    private static final Logger log = Logger.getLogger(MqttSinkQosTestCase.class);
    private static final  Server mqttBroker = new Server();
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
    public void mqttPublishEventsWithInvalidQos() throws Exception {
        try {
            log.info("Test for Mqtt Publish events with invalid QOS");
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "define stream FooStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@sink(type='mqtt', url= 'tcp://localhost:1883', " +
                            "topic='mqtt_publish_event_differ_qos', clean.session='true', message.retain='false', " +
                            "quality.of.service= '3', keep.alive= '60'," +
                            "@map(type='xml'))" +
                            "Define stream BarStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
            this.mqttTestClient = new MqttTestClient("tcp://localhost:1883",
                    "mqtt_publish_event_differ_qos", 1);

            siddhiAppRuntime.start();
            fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
            fooStream.send(new Object[]{"IBM", 75.6f, 100L});
            fooStream.send(new Object[]{"WSO2", 57.6f, 100L});

            Thread.sleep(10000);

            count = mqttTestClient.getCount();
            eventArrived = mqttTestClient.getEventArrived();

            AssertJUnit.assertEquals(3, count);
            AssertJUnit.assertTrue(eventArrived);
            siddhiAppRuntime.shutdown();
        } catch (Exception e) {
            log.warn("Qos value should takes 0 or 1 or 2");
        }

    }
}
