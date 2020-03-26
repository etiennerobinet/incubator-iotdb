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
import org.apache.camel.Processor;
import org.apache.camel.support.DefaultConsumer;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

public class IoTDBConsumer extends DefaultConsumer {

    private Session session;
    private final String query;
    private final IoTDBEndpoint endpoint;

    public IoTDBConsumer(IoTDBEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint=endpoint;
        session=endpoint.getSession();
        query=endpoint.getSql();
    }


    @Override
    public void doStart(){
        try {
            session.open();
            SessionDataSet rs =session.executeQueryStatement(query);
            Exchange ex = endpoint.createExchange();
            ex.getIn().setBody(rs);
            getProcessor().process(ex);

        } catch (Exception e) {
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
