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
package io.siddhi.extension.io.mqtt.source;

import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class MqttSourceTestCase {
    private static final Logger LOG = LogManager.getLogger(MqttSourceTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private int waitTime = 50;
    private int timeout = 60000;
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
    public void mqttReceiveSingleEvents() {
        LOG.info("Test for receive single events");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan2') "
                        + "define stream BarStream2 (symbol string, price float, volume long); "
                        + "@info(name = 'query1') "
                        + "@source(type='mqtt',url= 'tcp://localhost:1883',topic = 'mqtt_receive_single_event',"
                        + "clean.session='true', keep.alive= '60',@map(type='xml'))"
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
                + "topic='mqtt_receive_single_event', clean.session='true', message.retain='false', "
                + "quality.of.service= '1',keep.alive= '60'," + "@map(type='xml'))"
                + "Define stream BarStream (symbol string, price float, volume long);"
                + "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        try {
            fooStream.send(new Object[] { "WSO2", 55.6f, 100L });
            fooStream.send(new Object[] { "IBM", 75.6f, 100L });
            fooStream.send(new Object[] { "WSO2", 57.6f, 100L });
            SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was interrupted");
        }
        siddhiAppRuntimeSource.shutdown();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void mqttReceiveMultipleEvents() {
        LOG.info("test for receive muliple events");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan2') "
                        + "define stream BarStream2 (symbol string, price float, volume long); "
                        + "@info(name = 'query1') "
                        + "@source(type='mqtt',url= 'tcp://localhost:1883',topic = 'mqtt_receive_multiple_event',"
                        + "clean.session='true', keep.alive= '60', @map(type='xml'))"
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
                + "topic='mqtt_receive_multiple_event', clean.session='true', message.retain='false', "
                + "quality.of.service= '1',keep.alive= '60'," + "@map(type='xml'))"
                + "Define stream BarStream (symbol string, price float, volume long);"
                + "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        List<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[] { "WSO2", 55.6f, 100L }));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[] { "IBM", 75.6f, 100L }));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[] { "WSO2", 57.6f, 100L }));
        try {
            fooStream.send(arrayList.toArray(new Event[3]));
            SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was interrupted");
        }
        AssertJUnit.assertEquals(3, count.get());
        siddhiAppRuntimeSource.shutdown();
        siddhiAppRuntime.shutdown();

    }

    @Test(expectedExceptions = { SiddhiAppValidationException.class })
    public void testMqttWithoutUrl() {
        LOG.info("test for receive events without url");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan2') "
                        + "define stream BarStream2 (symbol string, price float, volume long); "
                        + "@info(name = 'query1') "
                        + "@source(type='mqtt',topic = 'mqtt_receive_without_url',clean.session='true',"
                        + " keep.alive= '60', @map(type='xml'))"
                        + "Define stream FooStream2 (symbol string, price float, volume long);"
                        + "from FooStream2 select symbol, price, volume insert into BarStream2;");
        siddhiAppRuntimeSource.start();
        siddhiAppRuntimeSource.shutdown();
    }

    @Test(expectedExceptions = { SiddhiAppValidationException.class })
    public void testMqttWithoutTopic() {
        LOG.info("test for receive events without topic");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan2') "
                        + "define stream BarStream2 (symbol string, price float, volume long); "
                        + "@info(name = 'query1') " + "@source(type='mqtt',url= 'tcp://localhost:1883',"
                        + "clean.session='true', keep.alive= '60', @map(type='xml'))"
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
        siddhiAppRuntimeSource.shutdown();
    }

    @Test
    public void mqttReceiveWithInvalidBrokerCredintials() {
        LOG.info("test for receive events invalid broker credintials");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan2') "
                        + "define stream BarStream2 (symbol string, price float, volume long); "
                        + "@info(name = 'query1') "
                        + "@source(type='mqtt',topic='mqtt_receive_with_invalid_url',url= 'tcp://localhost:1887',"
                        + "topic = 'mqtt_topic51'," + "clean.session='true', keep.alive= '60', @map(type='xml'))"
                        + "Define stream FooStream2 (symbol string, price float, volume long);"
                        + "from FooStream2 select symbol, price, volume insert into BarStream2;");
        siddhiAppRuntimeSource.start();
        siddhiAppRuntimeSource.shutdown();

    }

    @Test
    public void mqttReceiveWithoutCleanSession() {
        LOG.info("test for receive events without clean session");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan2') "
                        + "define stream BarStream2 (symbol string, price float, volume long); "
                        + "@info(name = 'query1') " + "@source(type='mqtt',url= 'tcp://localhost:1883',"
                        + "topic = 'mqtt_receive_without_clean_session'," + " keep.alive= '60', @map(type='xml'))"
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
                + "topic='mqtt_receive_without_clean_session', clean.session='true', message.retain='false', "
                + "quality.of.service= '1',keep.alive= '60'," + "@map(type='xml'))"
                + "Define stream BarStream (symbol string, price float, volume long);"
                + "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        List<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[] { "WSO2", 55.6f, 100L }));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[] { "IBM", 75.6f, 100L }));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[] { "WSO2", 57.6f, 100L }));
        try {
            fooStream.send(arrayList.toArray(new Event[3]));
            SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was interrupted");
        }
        AssertJUnit.assertEquals(3, count.get());
        siddhiAppRuntimeSource.shutdown();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void mqttReceiveWithoutKeepAlive() {
        LOG.info("test for receive events without keep Alive");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan2') "
                        + "define stream BarStream2 (symbol string, price float, volume long); "
                        + "@info(name = 'query1') "
                        + "@source(type='mqtt',url= 'tcp://localhost:1883',topic = 'mqtt_receive_without_keep_alive',"
                        + "clean.session='true', @map(type='xml'))"
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
                + "topic='mqtt_receive_without_keep_alive', clean.session='true', message.retain='false', "
                + "quality.of.service= '1',keep.alive= '60'," + "@map(type='xml'))"
                + "Define stream BarStream (symbol string, price float, volume long);"
                + "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        List<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[] { "WSO2", 55.6f, 100L }));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[] { "IBM", 75.6f, 100L }));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[] { "WSO2", 57.6f, 100L }));
        try {
            fooStream.send(arrayList.toArray(new Event[3]));
            SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was interrupted");
        }
        AssertJUnit.assertEquals(3, count.get());
        siddhiAppRuntimeSource.shutdown();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void mqttReceiveWithCleanSessionFalse() {
        LOG.info("test for receive events with clean session false");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan2') "
                        + "define stream BarStream2 (symbol string, price float, volume long); "
                        + "@info(name = 'query1') " + "@source(type='mqtt',url= 'tcp://localhost:1883',"
                        + "topic = 'mqtt_receive_with_clean_session_false',"
                        + "clean.session='false', @map(type='xml'))"
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
                + "topic='mqtt_receive_with_clean_session_false', clean.session='true', " + "message.retain='false', "
                + "quality.of.service= '1',keep.alive= '60'," + "@map(type='xml'))"
                + "Define stream BarStream (symbol string, price float, volume long);"
                + "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        List<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[] { "WSO2", 55.6f, 100L }));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[] { "IBM", 75.6f, 100L }));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[] { "WSO2", 57.6f, 100L }));
        try {
            fooStream.send(arrayList.toArray(new Event[3]));
            SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
            AssertJUnit.assertEquals(3, count.get());
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was interrupted");
        }
        siddhiAppRuntimeSource.shutdown();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void mqttRecieveEventsWithMultipleTopics() {
        LOG.info("Test for Mqtt Recieve events with Multiple topics");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan2') "
                        + "define stream BarStream2 (symbol string, price float, volume long); "
                        + "@info(name = 'query1') " + "@source(type='mqtt',url= 'tcp://localhost:1883',"
                        + "topic = 'mqtt/receives/MultipleTopics',"
                        + "quality.of.service='1',clean.session='true', keep.alive= '60',@map(type='xml'))"
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
                + "topic='mqtt/receives/MultipleTopics',quality.of.service='1', clean.session='true',"
                + " message.retain='false', " + "quality.of.service= '1',keep.alive= '60'," + "@map(type='xml'))"
                + "Define stream BarStream (symbol string, price float, volume long);"
                + "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        try {
            fooStream.send(new Object[] { "WSO2", 55.6f, 100L });
            fooStream.send(new Object[] { "IBM", 75.6f, 100L });
            fooStream.send(new Object[] { "WSO2", 57.6f, 100L });
            SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
            AssertJUnit.assertEquals(3, count.get());
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was interrupted");
        }
        siddhiAppRuntimeSource.shutdown();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void mqttRecieveEventsWithMultiLevel() {
        LOG.info("Test for Mqtt Recieve events with wildcard topics subscriptions with Multi level");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan2') "
                        + "define stream BarStream2 (symbol string, price float, volume long); "
                        + "@info(name = 'query1') "
                        + "@source(type='mqtt',url= 'tcp://localhost:1883',topic = 'mqtt/receive/#',"
                        + "quality.of.service='1',clean.session='true', keep.alive= '60',@map(type='xml'))"
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
                + "define stream FooStream (symbol string, price float, volume long); "
                + "define stream FooStream2 (symbol string, price float, volume long); " +

                "@info(name = 'query1') " + "@sink(type='mqtt', url= 'tcp://localhost:1883', "
                + "topic='mqtt/receive/MultipleTopics',quality.of.service='1', clean.session='true',"
                + " message.retain='false', " + "quality.of.service= '1',keep.alive= '60'," + "@map(type='xml'))"
                + "Define stream BarStream (symbol string, price float, volume long);" +

                "@info(name = 'query2') " + "@sink(type='mqtt', url= 'tcp://localhost:1883', "
                + "topic='mqtt/receive/SingleTopics',quality.of.service='1', clean.session='true',"
                + " message.retain='false', " + "quality.of.service= '1',keep.alive= '60'," + "@map(type='xml'))"
                + "Define stream BarStream2 (symbol string, price float, volume long);" +

                "from FooStream select symbol, price, volume insert into BarStream; "
                + "from FooStream2 select symbol, price, volume insert into BarStream2; ");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        InputHandler fooStream2 = siddhiAppRuntime.getInputHandler("FooStream2");
        siddhiAppRuntime.start();
        try {
            fooStream.send(new Object[] { "WSO2", 55.6f, 100L });
            fooStream2.send(new Object[] { "IBM", 75.6f, 100L });
            fooStream2.send(new Object[] { "WSO2", 57.6f, 100L });
            SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was interrupted");
        }
        AssertJUnit.assertEquals(3, count.get());
        siddhiAppRuntimeSource.shutdown();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void mqttRecieveEventsWithSingleLevel() {
        LOG.info("Test for Mqtt Recieve events with wildcard topics subscriptions with Single level");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan2') "
                        + "define stream BarStream2 (symbol string, price float, volume long); "
                        + "@info(name = 'query1') "
                        + "@source(type='mqtt',url= 'tcp://localhost:1883',topic = 'mqtt/+/Topic',"
                        + "quality.of.service='1',clean.session='true', keep.alive= '60',@map(type='xml'))"
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
                + "define stream FooStream (symbol string, price float, volume long); "
                + "define stream FooStream2 (symbol string, price float, volume long); " +

                "@info(name = 'query1') " + "@sink(type='mqtt', url= 'tcp://localhost:1883', "
                + "topic='mqtt/multiple/Topic',quality.of.service='1', clean.session='true',"
                + " message.retain='false', " + "quality.of.service= '1',keep.alive= '60'," + "@map(type='xml'))"
                + "Define stream BarStream (symbol string, price float, volume long);" +

                "@info(name = 'query2') " + "@sink(type='mqtt', url= 'tcp://localhost:1883', "
                + "topic='mqtt/single/Topic',quality.of.service='1', clean.session='true',"
                + " message.retain='false', " + "quality.of.service= '1',keep.alive= '60'," + "@map(type='xml'))"
                + "Define stream BarStream2 (symbol string, price float, volume long);" +

                "from FooStream select symbol, price, volume insert into BarStream; "
                + "from FooStream2 select symbol, price, volume insert into BarStream2; ");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        InputHandler fooStream2 = siddhiAppRuntime.getInputHandler("FooStream2");
        siddhiAppRuntime.start();
        try {
            fooStream.send(new Object[] { "WSO2", 55.6f, 100L });
            fooStream2.send(new Object[] { "IBM", 75.6f, 100L });
            fooStream2.send(new Object[] { "WSO2", 57.6f, 100L });
            SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was interrupted");
        }
        AssertJUnit.assertEquals(3, count.get());
        siddhiAppRuntimeSource.shutdown();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void mqttReceiveEvents() throws InterruptedException {
        LOG.info("Test for pause & resume");
        SiddhiManager siddhiManager = new SiddhiManager();

        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan2') "
                        + "define stream BarStream2 (symbol string, price float, volume long); "
                        + "@info(name = 'query1') "
                        + "@source(type='mqtt',url= 'tcp://localhost:1883',topic = 'mqtt_pause_resume',"
                        + "clean.session='true', keep.alive= '60',@map(type='xml'))"
                        + "Define stream FooStream2 (symbol string, price float, volume long);"
                        + "from FooStream2 select symbol, price, volume insert into BarStream2;");
        Collection<List<Source>> sources = siddhiAppRuntimeSource.getSources();
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
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime("@App:name('TestExecutionPlan') "
                + "define stream FooStream (symbol string, price float, volume long); " + "@info(name = 'query1') "
                + "@sink(type='mqtt', url= 'tcp://localhost:1883', "
                + "topic='mqtt_pause_resume', clean.session='true', message.retain='false', "
                + "quality.of.service= '1',keep.alive= '60'," + "@map(type='xml'))"
                + "Define stream BarStream (symbol string, price float, volume long);"
                + "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        List<Event> arrayList = new ArrayList<Event>();
        sources.forEach(e -> e.forEach(Source::pause));
        arrayList.clear();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[] { "WSO2", 55.6f, 100L }));
        try {
            fooStream.send(arrayList.toArray(new Event[1]));
            SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was interrupted");
        }
        arrayList.clear();
        sources.forEach(e -> e.forEach(Source::resume));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[] { "IBM", 75.6f, 100L }));
        try {
            fooStream.send(arrayList.toArray(new Event[1]));
            SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was interrupted");
        }
        arrayList.clear();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[] { "WSO2", 57.6f, 100L }));
        try {
            fooStream.send(arrayList.toArray(new Event[1]));
            SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was interrupted");
        }
        SiddhiTestHelper.waitForEvents(100, 3, count, 2000);
        AssertJUnit.assertEquals(3, count.get());
        siddhiAppRuntimeSource.shutdown();
        siddhiAppRuntime.shutdown();
    }
}
