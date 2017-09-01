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
package org.wso2.extension.siddhi.io.mqtt.source;

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
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;


public class MqttSourceTestCase {
    static final Logger LOG = Logger.getLogger(MqttSourceTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private int waitTime = 50;
    private int timeout = 30000;
    private volatile boolean eventArrived;
    private static final Server mqttBroker = new Server();


    @BeforeMethod
    public void initBeforeMethod() {
        count.set(0);
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
    public void mqttReceiveSingleEvents() throws InterruptedException, IOException, ConnectionUnavailableException {
        LOG.info("Test for receive single events");

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan2') " +
                        "define stream BarStream2 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@source(type='mqtt',url= 'tcp://localhost:1883',topic = 'mqtt_receive_single_event'," +
                        "clean.session='true', keep.alive= '60',@map(type='xml'))" +
                        "Define stream FooStream2 (symbol string, price float, volume long);" +
                        "from FooStream2 select symbol, price, volume insert into BarStream2;");
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
        Thread.sleep(4000);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type='mqtt', url= 'tcp://localhost:1883', " +
                        "topic='mqtt_receive_single_event', clean.session='true', message.retain='false', " +
                        "quality.of.service= '1',keep.alive= '60'," +
                        "@map(type='xml'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
        fooStream.send(new Object[]{"IBM", 75.6f, 100L});
        fooStream.send(new Object[]{"WSO2", 57.6f, 100L});
        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        AssertJUnit.assertEquals(3, count.get());
        siddhiAppRuntimeSource.shutdown();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void mqttReceiveMultipleEvents() throws InterruptedException, IOException, ConnectionUnavailableException {
        LOG.info("test for receive muliple events");

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan2') " +
                        "define stream BarStream2 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@source(type='mqtt',url= 'tcp://localhost:1883',topic = 'mqtt_receive_multiple_event'," +
                        "clean.session='true', keep.alive= '60', @map(type='xml'))" +
                        "Define stream FooStream2 (symbol string, price float, volume long);" +
                        "from FooStream2 select symbol, price, volume insert into BarStream2;");
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
        Thread.sleep(4000);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type='mqtt', url= 'tcp://localhost:1883', " +
                        "topic='mqtt_receive_multiple_event', clean.session='true', message.retain='false', " +
                        "quality.of.service= '1',keep.alive= '60'," +
                        "@map(type='xml'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        ArrayList<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100L}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100L}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 57.6f, 100L}));
        fooStream.send(arrayList.toArray(new Event[3]));
        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        AssertJUnit.assertEquals(3, count.get());
        siddhiAppRuntimeSource.shutdown();
        siddhiAppRuntime.shutdown();

    }


    @Test
    public void testMqttWithoutUrl() throws InterruptedException, IOException, ConnectionUnavailableException {
        LOG.info("test for receive events without url");
        try {
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan2') " +
                            "define stream BarStream2 (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='mqtt',topic = 'mqtt_receive_without_url',clean.session='true'," +
                            " keep.alive= '60', @map(type='xml'))" +
                            "Define stream FooStream2 (symbol string, price float, volume long);" +
                            "from FooStream2 select symbol, price, volume insert into BarStream2;");
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
            Thread.sleep(4000);
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream FooStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@sink(type='mqtt', url= 'tcp://localhost:1883', " +
                            "topic='mqtt_receive_without_url', clean.session='true', message.retain='false', " +
                            "quality.of.service= '1',keep.alive= '60'," +
                            "@map(type='xml'))" +
                            "Define stream BarStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
            siddhiAppRuntime.start();

            ArrayList<Event> arrayList = new ArrayList<Event>();
            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100L}));
            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100L}));
            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 57.6f, 100L}));
            fooStream.send(arrayList.toArray(new Event[3]));
            SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
            AssertJUnit.assertEquals(3, count.get());
            siddhiAppRuntimeSource.shutdown();
            siddhiAppRuntime.shutdown();
        } catch (Exception e) {
            LOG.warn("Error while connecting with the Mqtt Server ");
        }
    }

    @Test
    public void testMqttWithoutTopic() throws InterruptedException, IOException, ConnectionUnavailableException {
        LOG.info("test for receive events without topic");
        try {
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan2') " +
                            "define stream BarStream2 (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='mqtt',topic = 'mqtt_receive_without_topic',url= 'tcp://localhost:1883'," +
                            "clean.session='true', keep.alive= '60', @map(type='xml'))" +
                            "Define stream FooStream2 (symbol string, price float, volume long);" +
                            "from FooStream2 select symbol, price, volume insert into BarStream2;");
            siddhiAppRuntimeSource.addCallback("BarStream2", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        LOG.info(event);
                        eventArrived = true;
                        count.incrementAndGet();;
                    }
                }
            });
            siddhiAppRuntimeSource.start();
            Thread.sleep(4000);
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream FooStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@sink(type='mqtt', url= 'tcp://localhost:1883', " +
                            "topic='mqtt_receive_without_topic', clean.session='true', message.retain='false', " +
                            "quality.of.service= '1',keep.alive= '60'," +
                            "@map(type='xml'))" +
                            "Define stream BarStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
            siddhiAppRuntime.start();

            ArrayList<Event> arrayList = new ArrayList<Event>();
            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100L}));
            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100L}));
            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 57.6f, 100L}));
            fooStream.send(arrayList.toArray(new Event[3]));
            SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
            AssertJUnit.assertEquals(3, count.get());
            siddhiAppRuntimeSource.shutdown();
            siddhiAppRuntime.shutdown();

        } catch (Exception e) {
            LOG.warn("Mqtt topic is not provided ");
        }
    }

    @Test
    public void mqttReceiveWithInvalidBrokerCredintials() throws InterruptedException, IOException,
            ConnectionUnavailableException {
        LOG.info("test for receive events invalid broker credintials");
        try {
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan2') " +
                            "define stream BarStream2 (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='mqtt_receive_with_invalid_url',url= 'tcp://localhost:1887'," +
                            "topic = 'mqtt_topic51'," +
                            "clean.session='true', keep.alive= '60', @map(type='xml'))" +
                            "Define stream FooStream2 (symbol string, price float, volume long);" +
                            "from FooStream2 select symbol, price, volume insert into BarStream2;");
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
            Thread.sleep(4000);
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream FooStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@sink(type='mqtt', url= 'tcp://localhost:1883', " +
                            "topic='mqtt_receive_with_invalid_url', clean.session='true', message.retain='false', " +
                            "quality.of.service= '1',keep.alive= '60'," +
                            "@map(type='xml'))" +
                            "Define stream BarStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
            siddhiAppRuntime.start();

            ArrayList<Event> arrayList = new ArrayList<Event>();
            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100L}));
            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100L}));
            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 57.6f, 100L}));
            fooStream.send(arrayList.toArray(new Event[3]));
            SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
            AssertJUnit.assertEquals(3, count.get());
            siddhiAppRuntimeSource.shutdown();
            siddhiAppRuntime.shutdown();

        } catch (Exception e) {
            LOG.warn("invalid broker credintials ");
        }
    }

    @Test
    public void mqttReceiveWithoutCleanSession() throws InterruptedException,
            IOException, ConnectionUnavailableException {
        LOG.info("test for receive events without clean session");

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan2') " +
                        "define stream BarStream2 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@source(type='mqtt',url= 'tcp://localhost:1883'," +
                        "topic = 'mqtt_receive_without_clean_session'," +
                        " keep.alive= '60', @map(type='xml'))" +
                        "Define stream FooStream2 (symbol string, price float, volume long);" +
                        "from FooStream2 select symbol, price, volume insert into BarStream2;");
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
        Thread.sleep(4000);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type='mqtt', url= 'tcp://localhost:1883', " +
                        "topic='mqtt_receive_without_clean_session', clean.session='true', message.retain='false', " +
                        "quality.of.service= '1',keep.alive= '60'," +
                        "@map(type='xml'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        ArrayList<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100L}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100L}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 57.6f, 100L}));
        fooStream.send(arrayList.toArray(new Event[3]));
        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        AssertJUnit.assertEquals(3, count.get());
        siddhiAppRuntimeSource.shutdown();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void mqttReceiveWithoutKeepAlive() throws InterruptedException, IOException, ConnectionUnavailableException {
        LOG.info("test for receive events without keep Alive");

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan2') " +
                        "define stream BarStream2 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@source(type='mqtt',url= 'tcp://localhost:1883',topic = 'mqtt_receive_without_keep_alive'," +
                        "clean.session='true', @map(type='xml'))" +
                        "Define stream FooStream2 (symbol string, price float, volume long);" +
                        "from FooStream2 select symbol, price, volume insert into BarStream2;");
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
        Thread.sleep(4000);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type='mqtt', url= 'tcp://localhost:1883', " +
                        "topic='mqtt_receive_without_keep_alive', clean.session='true', message.retain='false', " +
                        "quality.of.service= '1',keep.alive= '60'," +
                        "@map(type='xml'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        ArrayList<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100L}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100L}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 57.6f, 100L}));
        fooStream.send(arrayList.toArray(new Event[3]));
        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        AssertJUnit.assertEquals(3, count.get());
        siddhiAppRuntimeSource.shutdown();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void mqttReceiveWithCleanSessionFalse() throws InterruptedException, IOException,
            ConnectionUnavailableException {
        LOG.info("test for receive events with clean session false");

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan2') " +
                        "define stream BarStream2 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@source(type='mqtt',url= 'tcp://localhost:1883'," +
                        "topic = 'mqtt_receive_with_clean_session_false'," +
                        "clean.session='false', @map(type='xml'))" +
                        "Define stream FooStream2 (symbol string, price float, volume long);" +
                        "from FooStream2 select symbol, price, volume insert into BarStream2;");
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
        Thread.sleep(4000);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type='mqtt', url= 'tcp://localhost:1883', " +
                        "topic='mqtt_receive_with_clean_session_false', clean.session='true', " +
                        "message.retain='false', " +
                        "quality.of.service= '1',keep.alive= '60'," +
                        "@map(type='xml'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        ArrayList<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100L}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100L}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 57.6f, 100L}));
        fooStream.send(arrayList.toArray(new Event[3]));
        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        AssertJUnit.assertEquals(3, count.get());
        siddhiAppRuntimeSource.shutdown();
        siddhiAppRuntime.shutdown();

    }
}
