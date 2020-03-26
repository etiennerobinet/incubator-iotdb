/*
Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 */
package org.apache.iotdb.camel;

import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultProducer;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.iotdb.session.Session;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IoTDBProducer extends DefaultProducer {

    private final String update;
    private Session session;
    public IoTDBProducer(IoTDBEndpoint endpoint) {
        super(endpoint);
        session = endpoint.getSession();
        update= endpoint.getSql();
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        /**Message in = exchange.getIn();
        String device = in.getHeader("device",String.class);
        List<String> sensor = in.getHeader("sensor",List.class);
        List<String> value = in.getHeader("value",List.class);
        session.insert(device,0,sensor,value);*/

        session.executeNonQueryStatement(update);
    }

    @Override
    public void doStart(){
        try {
            session.open();
        } catch (IoTDBSessionException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void doStop(){
        try {
            session.close();
        } catch (IoTDBSessionException e) {
            e.printStackTrace();
        }
    }

}
