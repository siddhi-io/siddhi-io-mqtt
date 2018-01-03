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

package org.wso2.carbon.event.input.adapter.mqtt;

import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapter;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapterConfiguration;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapterListener;
import org.wso2.carbon.event.input.adapter.core.exception.InputEventAdapterException;
import org.wso2.carbon.event.input.adapter.core.exception.TestConnectionNotSupportedException;
import org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTAdapterListener;
import org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTBrokerConnectionConfiguration;
import org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTEventAdapterConstants;

import java.util.Map;
import java.util.UUID;

/**
 * Input MQTTEventAdapter will be used to receive events with MQTT protocol using specified broker and topic.
 */
public class MQTTEventAdapter implements InputEventAdapter {

    private final InputEventAdapterConfiguration eventAdapterConfiguration;
    private final java.util.Map<String, String> globalProperties;
    private final String id = java.util.UUID.randomUUID().toString();
    private InputEventAdapterListener eventAdapterListener;
    private org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTAdapterListener mqttAdapterListener;
    private org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTBrokerConnectionConfiguration mqttBrokerConnectionConfiguration;

    public MQTTEventAdapter(InputEventAdapterConfiguration eventAdapterConfiguration,
                            java.util.Map<String, String> globalProperties) {
        this.eventAdapterConfiguration = eventAdapterConfiguration;
        this.globalProperties = globalProperties;
    }

    @Override
    public void init(InputEventAdapterListener eventAdapterListener) throws InputEventAdapterException {
        this.eventAdapterListener = eventAdapterListener;
        try {
            int keepAlive;

            //If global properties are available those will be assigned else constant values will be assigned
            if (globalProperties.get(
                    org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTEventAdapterConstants.ADAPTER_CONF_KEEP_ALIVE) != null) {
                keepAlive = Integer.parseInt((globalProperties.get(
                        org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTEventAdapterConstants.ADAPTER_CONF_KEEP_ALIVE)));
            } else {
                keepAlive = org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTEventAdapterConstants.ADAPTER_CONF_DEFAULT_KEEP_ALIVE;
            }

            mqttBrokerConnectionConfiguration = new org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTBrokerConnectionConfiguration(
                    eventAdapterConfiguration.getProperties().get(
                            org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTEventAdapterConstants.ADAPTER_CONF_URL),
                    eventAdapterConfiguration.getProperties().get(
                            org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTEventAdapterConstants
                                    .ADAPTER_CONF_USERNAME),
                    eventAdapterConfiguration.getProperties().get(
                            org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTEventAdapterConstants.ADAPTER_CONF_PASSWORD),
                    eventAdapterConfiguration.getProperties().get(
                            org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTEventAdapterConstants.ADAPTER_CONF_CLEAN_SESSION),
                    eventAdapterConfiguration.getProperties()
                            .get(org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTEventAdapterConstants.ADAPTER_CONNECTION_SSL_ENABLED),
                    String.valueOf(keepAlive));

            mqttAdapterListener = new org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTAdapterListener(mqttBrokerConnectionConfiguration, eventAdapterConfiguration
                    .getProperties().get(
                            org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTEventAdapterConstants.ADAPTER_MESSAGE_TOPIC), eventAdapterConfiguration
                    .getProperties().get(
                            org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTEventAdapterConstants.ADAPTER_CONF_CLIENTID), eventAdapterListener,
                                                                                                                 PrivilegedCarbonContext.getThreadLocalCarbonContext().
                                                                  getTenantId());
        } catch (Throwable t) {
            throw new InputEventAdapterException(t.getMessage(), t);
        }
    }

    @Override
    public void testConnect() throws TestConnectionNotSupportedException {
        throw new TestConnectionNotSupportedException("not-supported");
    }

    @Override
    public void connect() {
        mqttAdapterListener.createConnection();
    }

    @Override
    public void disconnect() {
        if (mqttAdapterListener != null) {
            mqttAdapterListener.stopListener(eventAdapterConfiguration.getName());
        }
    }

    @Override
    public void destroy() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof org.wso2.carbon.event.input.adapter.mqtt.MQTTEventAdapter)) {
            return false;
        }
        org.wso2.carbon.event.input.adapter.mqtt.MQTTEventAdapter that = (org.wso2.carbon.event.input.adapter.mqtt.MQTTEventAdapter) o;
        if (!id.equals(that.id)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean isEventDuplicatedInCluster() {
        return true;
    }

    @Override
    public boolean isPolling() {
        return true;
    }

}
