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
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;

public class MqttTestClient {
    private static final Logger log = Logger.getLogger(MqttTestClient.class);
    private MemoryPersistence persistence;
    private MqttClient client = null;
    private MqttConnectOptions connectionOptions = null;
    private String clientId;
    private String userName = null;
    private String userPassword = "";
    private boolean cleanSession = true;
    private boolean eventArrived;
    private int count;
    private int keepAlive = 60;
    private int connectionTimeout = 30;
    private MqttReceiverCallBack mqttReceiverCallBack;

    public class MqttReceiverCallBack implements MqttCallback {
        private boolean eventArrived = false;
        private int count = 0;

        public void connectionLost(Throwable throwable) {
        }

        public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
            eventArrived = true;
            count++;
        }

        public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        }

        public int getCount() {
            return count;
        }

        public boolean getEventArrived() {
            return eventArrived;
        }
    }


    public MqttTestClient(String brokerURL, String topic, int qos) throws ConnectionUnavailableException {
        try {
            persistence = new MemoryPersistence();
            clientId = MqttClient.generateClientId();
            client = new MqttClient(brokerURL, clientId, persistence);
            connectionOptions = new MqttConnectOptions();
            connectionOptions.setUserName(userName);
            connectionOptions.setPassword(userPassword.toCharArray());
            connectionOptions.setCleanSession(cleanSession);
            connectionOptions.setKeepAliveInterval(keepAlive);
            connectionOptions.setConnectionTimeout(connectionTimeout);
            client.connect(connectionOptions);
        } catch (MqttException e) {
            throw new ConnectionUnavailableException(
                    "Error in connecting with the Mqtt server" + e.getMessage(), e);
        }
        try {
            mqttReceiverCallBack = new MqttReceiverCallBack();
            client.setCallback(mqttReceiverCallBack);
            client.subscribe(topic, qos);
        } catch (MqttException e) {
            log.error("Error occurred when receiving message ", e);
        }
    }

    public int getCount() {
        return mqttReceiverCallBack.getCount();
    }

    public boolean getEventArrived() {
        return mqttReceiverCallBack.getEventArrived();
    }

    public void disconnectMqtt() {
        try {
            client.disconnect();
            log.debug("Disconnected from MQTT broker ");
        } catch (MqttException e) {
            log.error("Could not disconnect from MQTT broker ", e);
        } finally {
            try {
                client.close();
            } catch (MqttException e) {
                log.error("Could not close connection with MQTT broker ", e);
            }
        }
    }

}
