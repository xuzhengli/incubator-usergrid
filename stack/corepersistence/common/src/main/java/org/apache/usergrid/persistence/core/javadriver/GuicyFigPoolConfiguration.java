/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package org.apache.usergrid.persistence.core.javadriver;


import java.util.Arrays;
import java.util.Collection;

import org.apache.usergrid.persistence.core.astyanax.CassandraFig;

import com.google.inject.Inject;
import com.google.inject.Singleton;


/**
 *
 */
@Singleton
public class GuicyFigPoolConfiguration implements HystrixPoolConfiguration {

    private final CassandraFig cassandraFig;


    @Inject
    public GuicyFigPoolConfiguration( final CassandraFig cassandraFig ) {this.cassandraFig = cassandraFig;}


    @Override
    public String getServiceName() {
        return "Usergrid";
    }


    @Override
    public int getPort() {
        return cassandraFig.getNativePort();
    }


    @Override
    public void addPortLister( final PropertyChangeListener<Integer> listener ) {

    }


    @Override
    public Collection<String> getSeedNodes() {

        String hosts = cassandraFig.getHosts();

        String[] splitHosts = hosts.split( "," );

        return Arrays.asList( splitHosts );
    }


    @Override
    public void addSeedNodesLister( final PropertyChangeListener<Collection<String>> listener ) {

    }


    @Override
    public int getMaxConnectionsPerHost() {
        return this.cassandraFig.getConnections();
    }


    @Override
    public void addMaxConnectionsLister( final PropertyChangeListener<Integer> listener ) {

    }


    @Override
    public int getDefaultConnectionsPerIdentity() {
        /**
         * This is arbitrary and SUPER high.  TODO T.N. Make this come from a
         */
        return this.cassandraFig.getConcurrentInvocations();
    }


    @Override
    public void addDefaultConnectionsPerIdentity( final PropertyChangeListener<Integer> listener ) {

    }
}
