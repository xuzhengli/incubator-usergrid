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


import java.util.Collection;


/**
 * Simple interface to encapsulate configuration
 */
public interface HystrixPoolConfiguration {

    /**
     * Get the name of this service.  For instance Map/Entity etc
     */
    public String getServiceName();

    /**
     * Get the port to connect to in the Cassandra nodes
     */
    public int getPort();

    /**
     * Add the port change listener
     */
    public void addPortLister( final PropertyChangeListener<Integer> listener );

    /**
     * Get the collection of hostnames for the connection
     */
    public Collection<String> getSeedNodes();

    /**
     * Add the seed node change listener
     */
    public void addSeedNodesLister( final PropertyChangeListener<Collection<String>> listener );

    /*
     * Get the max connections to each Cassandra host
     */
    public int getMaxConnectionsPerHost();


    /**
     * Add the seed node change listener
     */
    public void addMaxConnectionsLister( final PropertyChangeListener<Integer> listener );


    /**
     * Get the default max connections per identity
     */
    public int getDefaultConnectionsPerIdentity();


    /**
     * Add the default max connections listener
     */
    public void addDefaultConnectionsPerIdentity( final PropertyChangeListener<Integer> listener );
}
