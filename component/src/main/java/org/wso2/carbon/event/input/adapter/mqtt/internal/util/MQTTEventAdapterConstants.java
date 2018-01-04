/*
*  Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

/**
 * This class represents MQTT specific Constants.
 */
public class MQTTEventAdapterConstants {

    public static final String ADAPTER_TYPE_MQTT = "mqtt";
    public static final String ADAPTER_CONF_URL = "url";
    public static final String ADAPTER_CONF_USERNAME = "username";
    public static final String ADAPTER_CONF_USERNAME_HINT = "username.hint";
    public static final String ADAPTER_CONF_PASSWORD = "password";
    public static final String ADAPTER_CONF_PASSWORD_HINT = "password.hint";
    public static final String ADAPTER_CONF_URL_HINT = "url.hint";
    public static final String ADAPTER_MESSAGE_TOPIC = "topic";
    public static final String ADAPTER_MESSAGE_TOPIC_HINT = "topic.hint";
    public static final String ADAPTER_CONF_CLIENTID = "clientId";
    public static final String ADAPTER_CONF_CLIENTID_HINT = "clientId.hint";
    public static final String ADAPTER_CONF_CLEAN_SESSION = "cleanSession";
    public static final String ADAPTER_CONF_CLEAN_SESSION_HINT = "cleanSession.hint";
    public static final String ADAPTER_CONF_KEEP_ALIVE = "keepAlive";
    public static final int ADAPTER_CONF_DEFAULT_KEEP_ALIVE = 60000;
    public static final String ADAPTER_CONNECTION_SSL_ENABLED = "connection.sslEnabled";
    public static final String ADAPTER_CONNECTION_SSL_ENABLED_HINT = "connection.sslEnabled.hint";
    public static final String ADAPTER_CONNECTION_SSL_TRUSTSTORE_LOCATION = System.getProperty("carbon.home") +
            java.io.File.separator + "repository" + java.io.File.separator + "resources" + java.io.File.separator
            + "security" + java.io.File.separator + "wso2carbon.jks";
    public static final String ADAPTER_CONNECTION_SSL_TRUSTSTORE_TYPE = "JKS";
    public static final String ADAPTER_CONNECTION_SSL_VERSION = "TLSv1";
    public static final String ADAPTER_CONNECTION_SSL_TRUSTSTORE_PASSWORD = "wso2carbon";
    public static final int reconnectionProgressionFactor = 2;
    public static int initialReconnectDuration = 10000;
}

