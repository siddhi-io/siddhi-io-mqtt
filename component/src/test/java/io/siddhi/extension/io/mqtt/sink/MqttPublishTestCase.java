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

import io.moquette.server.Server;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.input.InputHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.rmi.RemoteException;
import java.util.Properties;

public class MqttPublishTestCase {
    private volatile int count;
    private volatile boolean eventArrived;
    private static final Logger log = LogManager.getLogger(MqttSinkTestCase.class);
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
        ResultContainer resultContainer = new ResultContainer(3);
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
            this.mqttTestClient = new MqttTestClient("tcp://localhost:1883", "mqtt_publish_event", 1, resultContainer);
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
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("WSO2"));
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("IBM"));
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("WSO2"));
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("IBM"));
        siddhiAppRuntime.shutdown();
    }
}
