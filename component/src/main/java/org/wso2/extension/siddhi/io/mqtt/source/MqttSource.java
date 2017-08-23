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

import org.apache.log4j.Logger;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.wso2.extension.siddhi.io.mqtt.util.MqttConstants;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;

import java.util.Map;

/**
 * {@code MqttSource } Handle the Mqtt receiving tasks.
 */

@Extension(
        name = "mqtt",
        namespace = "source",
        description = "The MQTT source receives the events from an MQTT broker ",
        parameters = {
                @Parameter(
                        name = "url",
                        description = "The URL of the MQTT broker. It is used to connect to the MQTT broker. It is" +
                                " required to specify a valid URL here.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "username",
                        description = "The username to be provided when the MQTT client is authenticated by the " +
                                "broker.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "null"),
                @Parameter(
                        name = "password",
                        description = "The password to be provided when the MQTT client is authenticated by the " +
                                "broker.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "empty"),
                @Parameter(
                        name = "client.id",
                        description = "A unique ID for the MQTT client. The server uses this to identify the client " +
                                "when it reconnects. If you do not specify a client ID, the system automatically " +
                                "generates it.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "topic",
                        description = "The topic from which WSO2 SP receives events via MQTT. Multiple topics can " +
                                "be specified as a list of comma separated values." +
                                "This is a mandatory parameter.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "quality.of.service",
                        description = "The quality of service provided by the MQTT client. The possible values are " +
                                "as follows." +
                                "`0`: The MQTT client sends each event to WSO2 SP only once. It does not receive " +
                                "an acknowledgement when an event is delivered, and the events are not stored." +
                                "Events may get lost if the MQTT client is disconnected or if the server fails." +
                                "This is the fastest method in which events are received via MQTT." +
                                "`1`: The MQTT client sends each event to WSO2 SP at least once. If the MQTT client " +
                                "does not receive an acknowledgement to indicate that the event is delivered, it " +
                                "sends the event again." +
                                "`2`: The MQTT client sends each event to WSO2 SP only once. The events are stored " +
                                "until the WSO2 SP receives them. This is the safest, but the slowest method of " +
                                "receiving events via MQTT.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "1"),
                @Parameter(
                        name = "clean.session",
                        description = "clean session flag indicates the broker, whether the client wants " +
                                "to establish a persistent session or not,if clean session is set to false," +
                                " then the connection is treated as durable.If clean session is true, then " +
                                "all subscriptions will be removed for the client when it disconnects." +
                                "If the value provided in the MQTT source configuration is different to the values " +
                        "specified above, an error is logged when the Siddhi Application that contains the " +
                        "configuration is deployed.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "true"
                ),
                @Parameter(
                        name = "keep.alive",
                        description = "The maximum number of seconds the connection between the MQTT client and " +
                                "the broker should be maintained without any events being transferred. Once this " +
                                "time interval elapses without any event transfers, the connection is dropped. The " +
                                "default value is 60.",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "60"),
                @Parameter(
                        name = "connection.timeout",
                        description = "The maximum number of seconds that the MQTT client should spend attempting " +
                                "to connect to the MQTT broker. Once this time interval elapses, a timeout takes " +
                                "place.",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "30")
        },
        examples =
                {
                        @Example(
                                syntax = "@source(type='mqtt', url= 'tcp://localhost:1883', " +
                                        "topic='mqtt_topic', clean.session='true'," +
                                        "quality.of.service= '1', keep.alive= '60',connection.timeout='30'" +
                                        "@map(type='xml'))" +
                                        "Define stream BarStream (symbol string, price float, volume long);",
                                description = "This query receives events from the `mqtt_topic` topic via MQTT," +
                                "and processes them to the BarStream stream.")
                }
)

public class MqttSource extends Source {
    private static final Logger log = Logger.getLogger(MqttSource.class);

    private String brokerURL;
    private String topicOption;
    private String clientId;
    private String userName;
    private String userPassword;
    private String qosOption;
    private boolean cleanSession;
    private int keepAlive;
    private int connectionTimeout;
    private MqttClient client;
    private MqttConsumer mqttConsumer;
    private String siddhiAppName;


    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder, String[] strings,
                     ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        siddhiAppName = siddhiAppContext.getName();
        this.brokerURL = optionHolder.validateAndGetStaticValue(MqttConstants.MQTT_BROKER_URL);
        this.clientId = optionHolder.validateAndGetStaticValue(MqttConstants.CLIENT_ID,
                MqttConstants.EMPTY_STRING);
        this.topicOption = optionHolder.validateAndGetStaticValue(MqttConstants.MESSAGE_TOPIC);
        this.userName = optionHolder.validateAndGetStaticValue(MqttConstants.MQTT_BROKER_USERNAME,
                MqttConstants.DEFAULT_USERNAME);
        this.userPassword = optionHolder.validateAndGetStaticValue(MqttConstants.MQTT_BROKER_PASSWORD,
                MqttConstants.EMPTY_STRING);
        this.qosOption = optionHolder.validateAndGetStaticValue(MqttConstants.MESSAGE_QOS, MqttConstants.DEFAULT_QOS);
        this.keepAlive = Integer.parseInt(optionHolder.validateAndGetStaticValue
                (MqttConstants.CONNECTION_KEEP_ALIVE_INTERVAL,
                        MqttConstants.DEFAULT_CONNECTION_KEEP_ALIVE_INTERVAL));
        this.connectionTimeout = Integer.parseInt(optionHolder.validateAndGetStaticValue
                (MqttConstants.CONNECTION_TIMEOUT_INTERVAL,
                        MqttConstants.DEFAULT_CONNECTION_TIMEOUT_INTERVAL));
        this.cleanSession = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue
                (MqttConstants.CLEAN_SESSION, MqttConstants.DEFAULT_CLEAN_SESSION));
        this.mqttConsumer = new MqttConsumer(sourceEventListener);
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class};
    }

    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {
        try {
            MqttDefaultFilePersistence persistence = new MqttDefaultFilePersistence();
            if (clientId.equals(MqttConstants.EMPTY_STRING)) {
                clientId = MqttClient.generateClientId();
            }
            client = new MqttClient(brokerURL, clientId, persistence);
            MqttConnectOptions connectionOptions = new MqttConnectOptions();
            connectionOptions.setUserName(userName);
            connectionOptions.setPassword(userPassword.toCharArray());
            connectionOptions.setCleanSession(cleanSession);
            connectionOptions.setKeepAliveInterval(keepAlive);
            connectionOptions.setConnectionTimeout(connectionTimeout);
            client.connect(connectionOptions);
            int qos = Integer.parseInt(String.valueOf(qosOption));
            mqttConsumer.subscribe(topicOption, qos, client);
        } catch (MqttException e) {
            throw new ConnectionUnavailableException(
                    "Error while connecting with the Mqtt server. Check the url = " + brokerURL +
                            " defined in Siddhi App: " +
                           siddhiAppName , e);
        }
    }

    public void disconnect() {
        try {
            client.disconnect();
            log.debug("Disconnected from MQTT broker: " + brokerURL + " defined in Siddhi App: " +
                    siddhiAppName);
        } catch (MqttException e) {
            log.error("Could not disconnect from MQTT broker: " + brokerURL + " defined in Siddhi App: " +
                    siddhiAppName, e);
        } finally {
            try {
                client.close();
            } catch (MqttException e) {
                log.error("Could not close connection with MQTT broker: " + brokerURL +
                        " defined in Siddhi App: " +
                        siddhiAppName, e);
            }
        }

    }


    public void destroy() {

    }

    public void pause() {
        mqttConsumer.pause();

    }

    public void resume() {
        mqttConsumer.resume();

    }

    public Map<String, Object> currentState() {
        return null;
    }

    public void restoreState(Map<String, Object> map) {

    }
}
