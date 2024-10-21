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

package io.siddhi.extension.io.mqtt.util;
/**
 * {@code MqttSinkConstant }MQTT Sink Constants.
 */
public class MqttConstants {
    private MqttConstants() {
    }

    public static final String MQTT_BROKER_URL = "url";
    public static final String MESSAGE_TOPIC = "topic";
    public static final String MQTT_BROKER_USERNAME = "username";
    public static final String MQTT_BROKER_PASSWORD = "password";
    public static final String MESSAGE_QOS = "quality.of.service";
    public static final String CLEAN_SESSION = "clean.session";
    public static final String DEFAULT_CLEAN_SESSION = "true";
    public static final String CONNECTION_KEEP_ALIVE_INTERVAL = "keep.alive";
    public static final String DEFAULT_CONNECTION_KEEP_ALIVE_INTERVAL = "60";
    public static final String DEFAULT_QOS = "1";
    public static final String DEFAULT_MESSAGE_RETAIN = "false";
    public static final String MQTT_MESSAGE_RETAIN = "message.retain";
    public static final String CLIENT_ID = "client.id";
    public static final String EMPTY_STRING = "";
    public static final String DEFAULT_USERNAME = null;
    public static final String CONNECTION_TIMEOUT_INTERVAL = "connection.timeout";
    public static final String DEFAULT_CONNECTION_TIMEOUT_INTERVAL = "30";
    public static final String MAX_INFLIGHT = "max.inflight";
    public static final String DEFAULT_MAX_INFLIGHT = "10";
    public static final String AUTOMATIC_RECONNECT = "automatic.reconnect";
    public static final String DEFAULT_AUTOMATIC_RECONNECT = "false";
    public static final String MAX_RECONNECT_DELAY = "max.reconnect.delay";
    public static final String DEFAULT_MAX_RECONNECT_DELAY = "128000";
}
