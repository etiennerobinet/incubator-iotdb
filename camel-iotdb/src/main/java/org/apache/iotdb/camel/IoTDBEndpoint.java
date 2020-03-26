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

import org.apache.camel.support.DefaultEndpoint;
import org.apache.iotdb.camel.IoTDBConsumer;
import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.api.management.ManagedAttribute;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriPath;
import org.apache.iotdb.session.Session;

import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@UriEndpoint(scheme = "iotdb", title = "IoTDB", syntax = "iotdb:url", label = "iotdb")
public class IoTDBEndpoint extends DefaultEndpoint {
    @UriPath @Metadata(required = true)
    private String url;

    @UriParam @Metadata(required =false)
    private  String sql;

    @UriParam
    @Metadata(required = false)
    @SuppressWarnings("unused")
    private String username;

    @UriParam
    @Metadata(required = false)
    @SuppressWarnings("unused")
    private String password;

    private final int IOTDB_PORT=6667;
    private Session session;

    private final Pattern ipAdd = Pattern.compile("(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)");


    public IoTDBEndpoint(String uri, Component ioTDBComponent) {
        super(uri, ioTDBComponent);
        url = extractIp(uri);
        if (username != null && password != null) {
            session = new Session(url, IOTDB_PORT, username, password);
        } else {
            session = new Session(url, IOTDB_PORT, "root", "root");
        }
    }

    private String extractIp(String uri) {
        Matcher matcher = ipAdd.matcher(uri);
        if(matcher.find()){
            return matcher.group();
        }
        else return "0.0.0.0";
    }

    @Override
    public void doStart() throws Exception {
        super.doStart();
        if(url==null){
            url=getEndpointUri();
            System.out.println("URL: "+url);
        }
    }

    @ManagedAttribute
    public String getSql() {
        return sql;
    }

    @ManagedAttribute
    public void setSql(String sql) throws UnsupportedEncodingException {
        this.sql = java.net.URLDecoder.decode(sql, "UTF-8");
    }

    public Session getSession() {
        return session;
    }

    @Override
    public Producer createProducer() throws Exception {
        return new IoTDBProducer(this);
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        return new IoTDBConsumer(this,processor);
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @ManagedAttribute
    public String getUrl() {
        return url;
    }

    @ManagedAttribute
    public void setUrl(String url) {
        this.url = url;
    }

    @ManagedAttribute
    public String getUsername() {
        return username;
    }

    @ManagedAttribute
    public void setUsername(String username) {
        this.username = username;
    }

    @ManagedAttribute
    public String getPassword() {
        return password;
    }

    @ManagedAttribute
    public void setPassword(String password) {
        this.password = password;
    }
}
