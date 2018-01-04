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

package org.wso2.carbon.event.input.adapter.mqtt;

import org.wso2.carbon.event.input.adapter.core.InputEventAdapter;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapterConfiguration;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapterFactory;
import org.wso2.carbon.event.input.adapter.core.MessageType;
import org.wso2.carbon.event.input.adapter.core.Property;
import org.wso2.carbon.event.input.adapter.mqtt.internal.util.MQTTEventAdapterConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * This class represents the properties we need to get from the UI configuration for define Receiver.
 */
public class MQTTEventAdapterFactory extends InputEventAdapterFactory {

    private ResourceBundle resourceBundle = ResourceBundle.getBundle
            ("org.wso2.carbon.event.input.adapter.mqtt.i18n.Resources", Locale.getDefault());

    /**
     * This method returns the receiver type as a String.
     *
     * @return Type of the Adapter
     */
    @Override
    public String getType() {
        return MQTTEventAdapterConstants.ADAPTER_TYPE_MQTT;
    }

    /**
     * Specify supported message formats for the created receiver type.
     *
     * @return Supported Message Formats
     */
    @Override
    public List<String> getSupportedMessageFormats() {
        List<String> supportInputMessageTypes = new ArrayList<String>();
        supportInputMessageTypes.add(MessageType.TEXT);
        supportInputMessageTypes.add(MessageType.JSON);
        supportInputMessageTypes.add(MessageType.XML);

        return supportInputMessageTypes;
    }

    /**
     * Here the properties have to be defined for the receiver.
     * When defining properties you can implement to configure property values from the management console.
     *
     * @return List of Properties
     */
    @Override
    public List<Property> getPropertyList() {
        List<Property> propertyList = new ArrayList<org.wso2.carbon.event.input.adapter.core.Property>();

        // set topic
        Property topicProperty = new Property(MQTTEventAdapterConstants.ADAPTER_MESSAGE_TOPIC);
        topicProperty.setDisplayName(resourceBundle.getString(MQTTEventAdapterConstants.ADAPTER_MESSAGE_TOPIC));
        topicProperty.setRequired(true);
        topicProperty.setHint(resourceBundle.getString(MQTTEventAdapterConstants.ADAPTER_MESSAGE_TOPIC_HINT));
        propertyList.add(topicProperty);

        //Broker Url
        Property brokerUrl = new Property(MQTTEventAdapterConstants.ADAPTER_CONF_URL);
        brokerUrl.setDisplayName(resourceBundle.getString(MQTTEventAdapterConstants.ADAPTER_CONF_URL));
        brokerUrl.setRequired(true);
        brokerUrl.setHint(resourceBundle.getString(MQTTEventAdapterConstants.ADAPTER_CONF_URL_HINT));
        propertyList.add(brokerUrl);

        //Broker Username
        Property userName = new Property(MQTTEventAdapterConstants.ADAPTER_CONF_USERNAME);
        userName.setDisplayName(resourceBundle.getString(MQTTEventAdapterConstants.ADAPTER_CONF_USERNAME));
        userName.setRequired(false);
        userName.setHint(resourceBundle.getString(MQTTEventAdapterConstants.ADAPTER_CONF_USERNAME_HINT));
        propertyList.add(userName);

        //Broker Password
        Property password = new Property(MQTTEventAdapterConstants.ADAPTER_CONF_PASSWORD);
        password.setDisplayName(resourceBundle.getString(MQTTEventAdapterConstants.ADAPTER_CONF_PASSWORD));
        password.setSecured(true);
        password.setEncrypted(true);
        password.setRequired(false);
        password.setHint(resourceBundle.getString(MQTTEventAdapterConstants.ADAPTER_CONF_PASSWORD_HINT));
        propertyList.add(password);

        //Broker SSL Connection
        Property connectionSSLEnabled = new Property(MQTTEventAdapterConstants.ADAPTER_CONNECTION_SSL_ENABLED);
        connectionSSLEnabled.setDisplayName(resourceBundle.getString(MQTTEventAdapterConstants
                                                                             .ADAPTER_CONNECTION_SSL_ENABLED));
        connectionSSLEnabled.setRequired(false);
        connectionSSLEnabled.setOptions(new String[]{"true", "false"});
        connectionSSLEnabled.setDefaultValue("false");
        connectionSSLEnabled.setHint(resourceBundle.getString(MQTTEventAdapterConstants
                                                                      .ADAPTER_CONNECTION_SSL_ENABLED_HINT));
        propertyList.add(connectionSSLEnabled);

        //Broker clear session
        Property clearSession = new Property(MQTTEventAdapterConstants.ADAPTER_CONF_CLEAN_SESSION);
        clearSession.setDisplayName(
                resourceBundle.getString(MQTTEventAdapterConstants.ADAPTER_CONF_CLEAN_SESSION));
        clearSession.setRequired(false);
        clearSession.setOptions(new String[]{"true", "false"});
        clearSession.setDefaultValue("true");
        clearSession.setHint(resourceBundle.getString(MQTTEventAdapterConstants.ADAPTER_CONF_CLEAN_SESSION_HINT));
        propertyList.add(clearSession);

        // set clientId
        Property clientId = new Property(MQTTEventAdapterConstants.ADAPTER_CONF_CLIENTID);
        clientId.setDisplayName(resourceBundle.getString(MQTTEventAdapterConstants.ADAPTER_CONF_CLIENTID));
        clientId.setRequired(false);
        clientId.setHint(resourceBundle.getString(MQTTEventAdapterConstants.ADAPTER_CONF_CLIENTID_HINT));
        propertyList.add(clientId);

        return propertyList;
    }

    /**
     * Specify any hints to be displayed in the management console.
     *
     * @return Usage Tips
     */
    @Override
    public String getUsageTips() {
        return null;
    }

    /**
     * This method creates the receiver by specifying event adapter configuration
     * and global properties which are common to every adapter type.
     *
     * @param eventAdapterConfiguration Configuration of Adapter
     * @param globalProperties          Global properties from the map
     * @return MQTTEventAdapter
     */
    @Override
    public InputEventAdapter createEventAdapter(InputEventAdapterConfiguration eventAdapterConfiguration,
                                                Map<String, String> globalProperties) {
        return new MQTTEventAdapter(eventAdapterConfiguration, globalProperties);
    }
}

