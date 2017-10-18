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
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.persistence.InMemoryPersistenceStore;
import org.wso2.siddhi.core.util.persistence.PersistenceStore;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MqttSinkTestCase {
    private volatile int count;
    private volatile boolean eventArrived;
    private static final Logger log = Logger.getLogger(MqttSinkTestCase.class);
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
    public void mqttPublishSingleEvent() {
        log.info("Mqtt Publish test for single event");
        SiddhiManager siddhiManager = new SiddhiManager();
        ResultContainer resultContainer = new ResultContainer(3);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (symbol string, price float, volume long); " + "@info(name = 'query1') "
                        + "@sink(type='mqtt', url= 'tcp://localhost:1883', "
                        + "topic='mqtt_publish_single_event',username='mqtt-user', "
                        + "password='mqtt-password', clean.session='true', message.retain='false', "
                        + "quality.of.service= '1', keep.alive= '60'," + "@map(type='xml'))"
                        + "Define stream BarStream (symbol string, price float, volume long);"
                        + "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        try {
            this.mqttTestClient = new MqttTestClient("tcp://localhost:1883", "mqtt_publish_single_event", 1,
                    resultContainer);
        } catch (ConnectionUnavailableException e) {
            AssertJUnit.fail("Could not connect to broker.");
        }
        siddhiAppRuntime.start();
        try {
            fooStream.send(new Object[] { "WSO2", 55.6f, 100L });
            fooStream.send(new Object[] { "IBM", 75.6f, 100L });
            fooStream.send(new Object[] { "JAMES", 75.6f, 100L });
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was  interrupted");
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
    public void mqttPublishMultipleEvents() {
        log.info("Mqtt Publish test for multiple events");
        SiddhiManager siddhiManager = new SiddhiManager();
        ResultContainer resultContainer = new ResultContainer(1);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (symbol string, price float, volume long); " + "@info(name = 'query1') "
                        + "@sink(type='mqtt', url= 'tcp://localhost:1883', "
                        + "topic='mqtt_publish_multiple_event',username='mqtt-user', "
                        + "password='mqtt-password', clean.session='true', message.retain='false', "
                        + "quality.of.service= '1',keep.alive= '60'," + "@map(type='xml'))"
                        + "Define stream BarStream (symbol string, price float, volume long);"
                        + "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        try {
           this.mqttTestClient = new MqttTestClient("tcp://localhost:1883", "mqtt_publish_multiple_event", 1,
                    resultContainer);
        } catch (ConnectionUnavailableException e) {
            AssertJUnit.fail("Could not connect to broker.");
        }
        siddhiAppRuntime.start();
        List<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[] { "single_topic", 55.6f, 100L }));
        try {
            fooStream.send(arrayList.toArray(new Event[1]));
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was interrupted");
        }
        count = mqttTestClient.getCount();
        eventArrived = mqttTestClient.getEventArrived();
        AssertJUnit.assertEquals(1, count);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("single_topic"));
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = { SiddhiAppValidationException.class })
    public void mqttPublishWithoutUrlTest() {
        log.info("Mqqt Publish without url test");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (symbol string, price float, volume long); " + "@info(name = 'query1') "
                        + "@sink(type='mqtt', " + "topic='mqtt_publish_without_url', clean.session='true',"
                        + "username='mqtt-user', " + "password='mqtt-password', message.retain='false', "
                        + "quality.of.service= '1',keep.alive= '60'," + "@map(type='xml'))"
                        + "Define stream BarStream (symbol string, price float, volume long);"
                        + "from FooStream select symbol, price, volume insert into BarStream;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void mqttPublishWithInvalidBrokerCredintials() {
        log.info("test for publish events with invalid broker credintials");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (symbol string, price float, volume long); " + "@info(name = 'query1') "
                        + "@sink(type='mqtt',topic='mqtt_publish_with_invalid_url', url= 'tcp://localhost:1889', "
                        + "clean.session='true'," + "username='mqtt-user',password='mqtt-password',"
                        + " message.retain='false', " + "quality.of.service= '1',keep.alive= '60',"
                        + "@map(type='xml'))" + "Define stream BarStream (symbol string, price float, volume long);"
                        + "from FooStream select symbol, price, volume insert into BarStream;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = { SiddhiAppValidationException.class })
    public void mqttPublishWithoutTopic() {
        log.info("test for publish without topic ");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (symbol string, price float, volume long); " + "@info(name = 'query1') "
                        + "@sink(type='mqtt', "
                        + "url= 'tcp://localhost:1883', clean.session='true', message.retain='false', "
                        + "quality.of.service= '1',keep.alive= '60'," + "username='mqtt-user',password='mqtt-password',"
                        + "@map(type='xml'))" + "Define stream BarStream (symbol string, price float, volume long);"
                        + "from FooStream select symbol, price, volume insert into BarStream;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void mqttPublishWithoutCleanSession() {
        log.info("Mqtt Publish test for without clean session");
        SiddhiManager siddhiManager = new SiddhiManager();
        ResultContainer resultContainer = new ResultContainer(3);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (symbol string, price float, volume long); " + "@info(name = 'query1') "
                        + "@sink(type='mqtt', url= 'tcp://localhost:1883', "
                        + "topic='mqtt_publish_without_clean_session', message.retain='false', "
                        + "quality.of.service= '1',"
                        + "username='mqtt-user',password='mqtt-password', keep.alive= '60'," + "@map(type='xml'))"
                        + "Define stream BarStream (symbol string, price float, volume long);"
                        + "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        try {
            this.mqttTestClient = new MqttTestClient("tcp://localhost:1883", "mqtt_publish_without_clean_session", 1,
                    resultContainer);
        } catch (ConnectionUnavailableException e) {
            AssertJUnit.fail("Could not connect to broker.");
        }
        siddhiAppRuntime.start();
        try {

            fooStream.send(new Object[] { "WSO2", 55.6f, 100L });
            fooStream.send(new Object[] { "IBM", 75.6f, 100L });
            fooStream.send(new Object[] { "WSO2", 57.6f, 100L });
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
    public void mqttPublishWithoutKeepAlive() {
        log.info("Mqtt Publish test for without keep alive");
        SiddhiManager siddhiManager = new SiddhiManager();
        ResultContainer resultContainer = new ResultContainer(3);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (symbol string, price float, volume long); " + "@info(name = 'query1') "
                        + "@sink(type='mqtt', url= 'tcp://localhost:1883', "
                        + "topic='mqtt_publish_without_keep_alive', message.retain='false', "
                        + "quality.of.service= '1',"
                        + "username='mqtt-user',password='mqtt-password', clean.session='true'," + "@map(type='xml'))"
                        + "Define stream BarStream (symbol string, price float, volume long);"
                        + "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        try {
            this.mqttTestClient = new MqttTestClient("tcp://localhost:1883", "mqtt_publish_without_keep_alive", 1,
                    resultContainer);
        } catch (ConnectionUnavailableException e) {
            AssertJUnit.fail("Could not connect to broker.");
        }
        try {
            siddhiAppRuntime.start();
            fooStream.send(new Object[] { "WSO2", 55.6f, 100L });
            fooStream.send(new Object[] { "IBM", 75.6f, 100L });
            fooStream.send(new Object[] { "WSO2", 57.6f, 100L });

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
    public void mqttPublishWithCleanSessionFalse() {
        log.info("Mqtt Publish test for with clean session false");
        SiddhiManager siddhiManager = new SiddhiManager();
        ResultContainer resultContainer = new ResultContainer(3);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (symbol string, price float, volume long); " + "@info(name = 'query1') "
                        + "@sink(type='mqtt', url= 'tcp://localhost:1883', "
                        + "topic='mqtt_publish_with_clean_session_false', message.retain='false', "
                        + "quality.of.service= '1',"
                        + "username='mqtt-user',password='mqtt-password', clean.session='false'," + "@map(type='xml'))"
                        + "Define stream BarStream (symbol string, price float, volume long);"
                        + "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        try {
            this.mqttTestClient = new MqttTestClient("tcp://localhost:1883", "mqtt_publish_with_clean_session_false",
                    1, resultContainer);
        } catch (ConnectionUnavailableException e) {
            AssertJUnit.fail("Could not connect to broker.");
        }
        siddhiAppRuntime.start();
        try {
            fooStream.send(new Object[] { "WSO2", 55.6f, 100L });
            fooStream.send(new Object[] { "IBM", 75.6f, 100L });
            fooStream.send(new Object[] { "WSO2", 57.6f, 100L });
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
    public void mqttPublishWithDifferentValueKeepAlive() {
        log.info("Mqtt Publish test for Diffferent values of keep.alive");
        SiddhiManager siddhiManager = new SiddhiManager();
        ResultContainer resultContainer = new ResultContainer(3);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (symbol string, price float, volume long); " + "@info(name = 'query1') "
                        + "@sink(type='mqtt', url= 'tcp://localhost:1883',username='mqtt-user',"
                        + "password='mqtt-password',"
                        + "topic='mqtt_publish_with_different_keep_alive', message.retain='false', "
                        + "quality.of.service= '1', clean.session='false', keep.alive= '80'," + "@map(type='xml'))"
                        + "Define stream BarStream (symbol string, price float, volume long);"
                        + "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");

        try {
            this.mqttTestClient = new MqttTestClient("tcp://localhost:1883", "mqtt_publish_with_different_keep_alive",
                    1, resultContainer);
        } catch (ConnectionUnavailableException e) {
            AssertJUnit.fail("Could not connect to broker.");
        }
        siddhiAppRuntime.start();
        try {
            fooStream.send(new Object[] { "WSO2", 55.6f, 100L });
            fooStream.send(new Object[] { "IBM", 75.6f, 100L });
            fooStream.send(new Object[] { "WSO2", 57.6f, 100L });
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
    }

    @Test
    public void mqttPublishWithoutBrokerCredentials() {
        log.info("Mqtt Publish test for Diffferent values of keep.alive");
        SiddhiManager siddhiManager = new SiddhiManager();
        ResultContainer resultContainer = new ResultContainer(3);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (symbol string, price float, volume long); " + "@info(name = 'query1') "
                        + "@sink(type='mqtt', url= 'tcp://localhost:1883',"
                        + "topic='mqtt_publish_without_broker_credentials', message.retain='false', "
                        + "quality.of.service= '1', clean.session='false', keep.alive= '60'," + "@map(type='xml'))"
                        + "Define stream BarStream (symbol string, price float, volume long);"
                        + "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");

        try {
            this.mqttTestClient = new MqttTestClient("tcp://localhost:1883", "mqtt_publish_without_broker_credentials",
                    1, resultContainer);
        } catch (ConnectionUnavailableException e) {
            AssertJUnit.fail("Could not connect to broker.");
        }
        siddhiAppRuntime.start();
        try {
            fooStream.send(new Object[] { "WSO2", 55.6f, 100L });
            fooStream.send(new Object[] { "IBM", 75.6f, 100L });
            fooStream.send(new Object[] { "WSO2", 57.6f, 100L });
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
    }

    @Test
    public void mqttPublishTest() {
        log.info("Test for persist and restore state ");
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        ResultContainer resultContainer = new ResultContainer(3);
        siddhiManager.setPersistenceStore(persistenceStore);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (symbol string, price float, volume long); " + "@info(name = 'query1') "
                        + "@sink(type='mqtt', url= 'tcp://localhost:1883', "
                        + "topic='mqtt_publish_event_persist',username='mqtt-user', "
                        + "password='mqtt-password', clean.session='true', message.retain='false', "
                        + "quality.of.service= '1', keep.alive= '60'," + "@map(type='xml'))"
                        + "Define stream BarStream (symbol string, price float, volume long);"
                        + "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        try {
            this.mqttTestClient = new MqttTestClient("tcp://localhost:1883", "mqtt_publish_event_persist", 1,
                    resultContainer);
        } catch (ConnectionUnavailableException e) {
            AssertJUnit.fail("Could not connect to broker.");
        }
        siddhiAppRuntime.start();
        try {
            fooStream.send(new Object[] { "WSO2", 55.6f, 100L });
            fooStream.send(new Object[] { "IBM", 75.6f, 100L });
            fooStream.send(new Object[] { "JAMES", 57.6f, 100L });
            Thread.sleep(500);
            siddhiAppRuntime.persist();
            siddhiAppRuntime.shutdown();
            fooStream = siddhiAppRuntime.getInputHandler("FooStream");
            siddhiAppRuntime.start();
            siddhiAppRuntime.restoreLastRevision();
            fooStream.send(new Object[] { "MIKE", 75.6f, 100L });
            Thread.sleep(500);

        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was  interrupted");
        }
        count = mqttTestClient.getCount();
        eventArrived = mqttTestClient.getEventArrived();
        AssertJUnit.assertEquals(4, count);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("WSO2"));
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("IBM"));
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("JAMES"));
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiAppRuntime.shutdown();
    }
}
