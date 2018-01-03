/*
*  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.carbon.event.input.adapter.mqtt.internal.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.core.ServerStatus;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapterListener;
import org.wso2.carbon.event.input.adapter.core.exception.InputEventAdapterRuntimeException;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.security.cert.CertificateException;
import java.security.NoSuchAlgorithmException;
import java.security.KeyStoreException;
import java.security.KeyManagementException;

public class MQTTAdapterListener implements org.eclipse.paho.client.mqttv3.MqttCallback, Runnable {

    private static final Log log = LogFactory.getLog(
            org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTAdapterListener.class);

    private org.eclipse.paho.client.mqttv3.MqttClient mqttClient;
    private org.eclipse.paho.client.mqttv3.MqttConnectOptions connectionOptions;
    private boolean cleanSession;
    private int keepAlive;
    private boolean connectionSSLEnabled;
    private String sslTrustStoreLocation;
    private String sslTrustStoreType;
    private String sslTrustStoreVersion;
    private String sslTrustStorePassword;
    private java.io.FileInputStream fileInputStream;

    private MQTTBrokerConnectionConfiguration mqttBrokerConnectionConfiguration;
    private String mqttClientId;
    private String topic;
    private int tenantId;
    private boolean connectionSucceeded = false;

    private InputEventAdapterListener eventAdapterListener = null;

    public MQTTAdapterListener(MQTTBrokerConnectionConfiguration mqttBrokerConnectionConfiguration,
                               String topic, String mqttClientId, InputEventAdapterListener inputEventAdapterListener,
                               int tenantId) throws java.io.IOException {

        if (mqttClientId == null || mqttClientId.trim().isEmpty()) {
            mqttClientId = org.eclipse.paho.client.mqttv3.MqttClient.generateClientId();
        }

        this.mqttClientId = mqttClientId;
        this.mqttBrokerConnectionConfiguration = mqttBrokerConnectionConfiguration;
        this.cleanSession = mqttBrokerConnectionConfiguration.isCleanSession();
        this.keepAlive = mqttBrokerConnectionConfiguration.getKeepAlive();
        this.connectionSSLEnabled = mqttBrokerConnectionConfiguration.isConnectionSSLEnabled();
        this.sslTrustStoreLocation = MQTTEventAdapterConstants.ADAPTER_CONNECTION_SSL_TRUSTSTORE_LOCATION;
        this.sslTrustStoreType = MQTTEventAdapterConstants.ADAPTER_CONNECTION_SSL_TRUSTSTORE_TYPE;
        this.sslTrustStoreVersion = MQTTEventAdapterConstants.ADAPTER_CONNECTION_SSL_VERSION;
        this.sslTrustStorePassword = MQTTEventAdapterConstants.ADAPTER_CONNECTION_SSL_TRUSTSTORE_PASSWORD;
        this.topic = topic;
        this.eventAdapterListener = inputEventAdapterListener;
        this.tenantId = tenantId;
        //SORTING messages until the server fetches them
        String temp_directory = System.getProperty("java.io.tmpdir");
        org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence dataStore = new org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence(temp_directory);

        try {
            // Construct the connection options object that contains connection parameters
            // such as cleanSession and LWT
            connectionOptions = new org.eclipse.paho.client.mqttv3.MqttConnectOptions();
            connectionOptions.setCleanSession(cleanSession);
            connectionOptions.setKeepAliveInterval(keepAlive);
            //Create the secure connection
            if (connectionSSLEnabled) {
                char[] trustStorePassword = sslTrustStorePassword.toCharArray();
                java.security.KeyStore keyStore = java.security.KeyStore.getInstance(sslTrustStoreType);
                fileInputStream = new java.io.FileInputStream(sslTrustStoreLocation);
                keyStore.load(fileInputStream, trustStorePassword);
                javax.net.ssl.TrustManagerFactory trustManagerFactory = javax.net.ssl.TrustManagerFactory.getInstance
                        (javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init(keyStore);
                javax.net.ssl.SSLContext context = javax.net.ssl.SSLContext.getInstance(sslTrustStoreVersion);
                context.init(null, trustManagerFactory.getTrustManagers(), null);
                connectionOptions.setSocketFactory(context.getSocketFactory());
            }
            if (this.mqttBrokerConnectionConfiguration.getBrokerPassword() != null) {
                connectionOptions.setPassword(this.mqttBrokerConnectionConfiguration.getBrokerPassword().toCharArray());
            }
            if (this.mqttBrokerConnectionConfiguration.getBrokerUsername() != null) {
                connectionOptions.setUserName(this.mqttBrokerConnectionConfiguration.getBrokerUsername());
            }

            // Construct an MQTT blocking mode client
            mqttClient = new org.eclipse.paho.client.mqttv3.MqttClient(this.mqttBrokerConnectionConfiguration.getBrokerUrl(), this.mqttClientId,
                                                                       dataStore);

            // Set this wrapper as the callback handler
            mqttClient.setCallback(this);

        } catch (org.eclipse.paho.client.mqttv3.MqttException e) {
            log.error("Exception occurred while subscribing to MQTT broker at "
                              + mqttBrokerConnectionConfiguration.getBrokerUrl());
            throw new InputEventAdapterRuntimeException(e);
        } catch (java.io.IOException e) {
            throw new InputEventAdapterRuntimeException
                    ("TrustStore File path is incorrect. Specify TrustStore location Correctly.", e);
        } catch (java.security.cert.CertificateException e) {
            throw new InputEventAdapterRuntimeException("TrustStore is not specified. So Security certificate" +
                                                                " Exception happened.  ", e);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new InputEventAdapterRuntimeException("Algorithm is not available in KeyManagerFactory class.", e);
        } catch (java.security.KeyStoreException e) {
            throw new InputEventAdapterRuntimeException("Error in TrustStore Type", e);
        } catch (java.security.KeyManagementException e) {
            throw new InputEventAdapterRuntimeException("Error in Key Management", e);
        } finally {
            if (fileInputStream != null) {
                fileInputStream.close();
            }
        }
    }

    public void startListener() throws org.eclipse.paho.client.mqttv3.MqttException {
        // Connect to the MQTT server
        mqttClient.connect(connectionOptions);

        // Subscribe to the requested topic
        // The QoS specified is the maximum level that messages will be sent to the client at.
        // For instance if QoS 1 is specified, any messages originally published at QoS 2 will
        // be downgraded to 1 when delivering to the client but messages published at 1 and 0
        // will be received at the same level they were published at.
        mqttClient.subscribe(topic);
    }

    public void stopListener(String adapterName) {
        if (connectionSucceeded) {
            try {
                // Un-subscribe accordingly and disconnect from the MQTT server.
                if (!ServerStatus.getCurrentStatus().equals(
                        ServerStatus.STATUS_SHUTTING_DOWN) || cleanSession) {
                    mqttClient.unsubscribe(topic);
                }
                mqttClient.disconnect(3000);
            } catch (org.eclipse.paho.client.mqttv3.MqttException e) {
                log.error("Can not unsubscribe from the destination " + topic
                                  + " with the event adapter " + adapterName, e);
            }
        }
        //This is to stop all running reconnection threads
        connectionSucceeded = true;
    }

    @Override
    public void connectionLost(Throwable throwable) {
        log.warn("MQTT connection not reachable " + throwable);
        connectionSucceeded = false;
        new Thread(this).start();
    }

    @Override
    public void messageArrived(String s, org.eclipse.paho.client.mqttv3.MqttMessage mqttMessage) throws Exception {
        try {
            String msgText = mqttMessage.toString();
            if (log.isDebugEnabled()) {
                log.debug(msgText);
            }
            PrivilegedCarbonContext.startTenantFlow();
            PrivilegedCarbonContext.getThreadLocalCarbonContext().setTenantId(tenantId);

            if (log.isDebugEnabled()) {
                log.debug("Event received in MQTT Event Adapter - " + msgText);
            }

            eventAdapterListener.onEvent(msgText);
        } catch (InputEventAdapterRuntimeException e) {
            throw new InputEventAdapterRuntimeException(e);
        } finally {
            PrivilegedCarbonContext.endTenantFlow();
        }
    }

    @Override
    public void deliveryComplete(org.eclipse.paho.client.mqttv3.IMqttDeliveryToken iMqttDeliveryToken) {

    }

    @Override
    public void run() {
        while (!connectionSucceeded) {
            try {
                MQTTEventAdapterConstants.initialReconnectDuration = MQTTEventAdapterConstants.initialReconnectDuration
                        * MQTTEventAdapterConstants.reconnectionProgressionFactor;
                Thread.sleep(MQTTEventAdapterConstants.initialReconnectDuration);
                startListener();
                connectionSucceeded = true;
                log.info("MQTT Connection successful");
            } catch (InterruptedException e) {
                log.error("Interruption occurred while waiting for reconnection", e);
            } catch (org.eclipse.paho.client.mqttv3.MqttException e) {
                log.error("MQTT Exception occurred when starting listener", e);
            }

        }
    }

    public void createConnection() {
        new Thread(this).start();
    }
}
