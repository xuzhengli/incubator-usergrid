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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;


/**
 * Implementation of the customer specific pool with capacity circuit breakers in place
 */
public class HystrixPoolImpl implements HystrixPool {


    private static final Logger logger = LoggerFactory.getLogger( HystrixPoolImpl.class );

    /**
     * This can be updated by another thread, we want to make it available
     */
    private volatile Cluster cluster;

    private final HystrixPoolConfiguration config;


    /**
     * Create an instance of the customer pool.  This starts the pool and verifies the connections when a new instance
     * is created
     * @param config
     */
    public HystrixPoolImpl( final HystrixPoolConfiguration config ) {
        this.config = config;
        buildFromConfiguration();
    }

    private void buildFromConfiguration(){

        Preconditions.checkNotNull( config, "config is required");
        Preconditions.checkNotNull( config.getServiceName(), "serviceName is required" );
        Preconditions.checkArgument( config.getServiceName().length() > 0, "serviceName must have a length > 0" );

        Preconditions.checkArgument( config.getPort() > 0, "Your port must be at least 1" );
        Preconditions.checkNotNull( config.getSeedNodes(), "You must provide at least one seed node" );
        Preconditions.checkArgument( config.getSeedNodes().size() > 0, "You must provide at least one seed node" );




        Cluster.Builder builder = Cluster.builder().withPort( config.getPort() );


        for ( String seedNode : config.getSeedNodes() ) {
            builder.addContactPoint( seedNode );
        }


        /**
         * Set the min and max equal so they're always open
         */
        PoolingOptions options = new PoolingOptions();


        options.setMaxConnectionsPerHost( HostDistance.LOCAL, config.getMaxConnectionsPerHost() );
        options.setMaxConnectionsPerHost( HostDistance.REMOTE, config.getMaxConnectionsPerHost() );


        builder.withPoolingOptions( options );

        /**
         * TODO add retry policy and load balancing semantics
         */

        cluster = builder.build();
        Metadata metadata = cluster.getMetadata();


        if ( logger.isInfoEnabled() ) {
            logger.info( "Connected to cluster: {}", metadata.getClusterName() );
            for ( Host host : metadata.getAllHosts() ) {
                logger.info( "Datacenter: {}; Host: {}; Rack: {}", host.getDatacenter(), host.getAddress(),
                        host.getRack() );
            }
        }

    }


    @Override
    public Session getSession( final String identifier ) {

        Preconditions.checkNotNull( identifier, "You must provide an identifier" );
        Preconditions.checkArgument( identifier.length() > 0, "You must provide an identifier with a length" );

        if ( cluster == null ) {
            throw new IllegalStateException( "You must start the pool before you can invoke getSession" );
        }

        return new HystrixSession( cluster.newSession(), config.getServiceName(), identifier, config.getDefaultConnectionsPerIdentity() );

    }


    @Override
    public void close() {
        if ( cluster.isClosed() ) {
            throw new IllegalStateException( "You cannot close the pool, it has not been started" );
        }

        cluster.close();
    }


}
