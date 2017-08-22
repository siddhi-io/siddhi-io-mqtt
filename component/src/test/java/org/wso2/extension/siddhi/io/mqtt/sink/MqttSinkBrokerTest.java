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

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.input.InputHandler;


public class MqttSinkBrokerTest {
    private volatile int count;
    private volatile boolean eventArrived;
    private static final Logger log = Logger.getLogger(MqttSinkBrokerTest.class);
    private MqttTestClient mqttTestClient;

    @BeforeMethod
    public void initBeforeMethod() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void mqttPublishWhenBrokerNotAvailable() {
        log.info("Test for receive events when broker is not available");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type='mqtt', url= 'tcp://localhost:1883', " +
                        "topic='mqtt_publish_broker_not_available', clean.session='true', " +
                        "message.retain='false', " +
                        "quality.of.service= '1', keep.alive= '60'," +
                        "@map(type='xml'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        try {
            this.mqttTestClient = new MqttTestClient("tcp://localhost:1883",
                    "mqtt_publish_broker_not_available", 1);
            AssertJUnit.fail("Expected ConnectionUnavailableException did not occur");

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

        } catch (ConnectionUnavailableException e) {
            //do nothing as this is expected.
        } catch (InterruptedException e) {
            AssertJUnit.fail("Failed to send messages");
        }

    }
}
