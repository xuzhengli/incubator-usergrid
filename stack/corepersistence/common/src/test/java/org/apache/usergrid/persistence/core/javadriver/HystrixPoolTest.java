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
import java.util.Collections;

import org.junit.ClassRule;
import org.junit.Test;

import org.apache.usergrid.persistence.core.cassandra.CassandraRule;

import com.datastax.driver.core.Session;



public class HystrixPoolTest {

    @ClassRule
    public static CassandraRule rule = new CassandraRule();


    @Test
    public void testStartAndStop() {

        final HystrixPoolConfiguration poolConfiguration = new HystrixPoolConfiguration() {

            @Override
            public String getServiceName() {
                return "test";
            }


            @Override
            public int getPort() {
                return rule.getCassandraFig().getNativePort();
            }


            @Override
            public void addPortLister( final PropertyChangeListener<Integer> listener ) {

            }


            @Override
            public Collection<String> getSeedNodes() {
                return Collections.singleton( "localhost" );
            }


            @Override
            public void addSeedNodesLister( final PropertyChangeListener<Collection<String>> listener ) {

            }


            @Override
            public int getMaxConnectionsPerHost() {
                return 20;
            }


            @Override
            public void addMaxConnectionsLister( final PropertyChangeListener<Integer> listener ) {

            }


            @Override
            public int getDefaultConnectionsPerIdentity() {
                return 10;
            }


            @Override
            public void addDefaultConnectionsPerIdentity( final PropertyChangeListener<Integer> listener ) {

            }
        };

        HystrixPool pool = new HystrixPoolImpl( poolConfiguration );

        Session session = pool.getSession( "test" );


        String keyspaceCreate = "CREATE KEYSPACE testStartAndStop WITH REPLICATION = { 'class' : 'SimpleStrategy', "
                + "'replication_factor' : 3 };";


        session.execute( keyspaceCreate );

        //close the client, has no effect on the shared pool
        session.close();

        pool.close();
    }


    @Test( expected = IllegalArgumentException.class )
    public void invalidService() {
        final HystrixPoolConfiguration poolConfiguration = new HystrixPoolConfiguration() {

            @Override
            public String getServiceName() {
                return "";
            }


            @Override
            public int getPort() {
                return 10;
            }


            @Override
            public void addPortLister( final PropertyChangeListener<Integer> listener ) {

            }


            @Override
            public Collection<String> getSeedNodes() {
                return Collections.singleton( "localhost" );
            }


            @Override
            public void addSeedNodesLister( final PropertyChangeListener<Collection<String>> listener ) {

            }


            @Override
            public int getMaxConnectionsPerHost() {
                return 20;
            }


            @Override
            public void addMaxConnectionsLister( final PropertyChangeListener<Integer> listener ) {

            }




            @Override
            public int getDefaultConnectionsPerIdentity() {
                return 10;
            }


            @Override
            public void addDefaultConnectionsPerIdentity( final PropertyChangeListener<Integer> listener ) {

            }
        };


        new HystrixPoolImpl( poolConfiguration );
    }


    @Test( expected = NullPointerException.class )
    public void nullService() {
        final HystrixPoolConfiguration poolConfiguration = new HystrixPoolConfiguration() {

            @Override
            public String getServiceName() {
                return null;
            }


            @Override
            public int getPort() {
                return 10;
            }


            @Override
            public void addPortLister( final PropertyChangeListener<Integer> listener ) {

            }


            @Override
            public Collection<String> getSeedNodes() {
                return Collections.singleton( "localhost" );
            }


            @Override
            public void addSeedNodesLister( final PropertyChangeListener<Collection<String>> listener ) {

            }


            @Override
            public int getMaxConnectionsPerHost() {
                return 20;
            }


            @Override
            public void addMaxConnectionsLister( final PropertyChangeListener<Integer> listener ) {

            }


            @Override
            public int getDefaultConnectionsPerIdentity() {
                return 10;
            }


            @Override
            public void addDefaultConnectionsPerIdentity( final PropertyChangeListener<Integer> listener ) {

            }
        };


        new HystrixPoolImpl( poolConfiguration );
    }


    @Test( expected = IllegalArgumentException.class )
    public void invalidPort() {
        final HystrixPoolConfiguration poolConfiguration = new HystrixPoolConfiguration() {

            @Override
            public String getServiceName() {
                return "test";
            }


            @Override
            public int getPort() {
                return 0;
            }


            @Override
            public void addPortLister( final PropertyChangeListener<Integer> listener ) {

            }


            @Override
            public Collection<String> getSeedNodes() {
                return Collections.singleton( "localhost" );
            }


            @Override
            public void addSeedNodesLister( final PropertyChangeListener<Collection<String>> listener ) {

            }


            @Override
            public int getMaxConnectionsPerHost() {
                return 20;
            }


            @Override
            public void addMaxConnectionsLister( final PropertyChangeListener<Integer> listener ) {

            }


            @Override
            public int getDefaultConnectionsPerIdentity() {
                return 10;
            }


            @Override
            public void addDefaultConnectionsPerIdentity( final PropertyChangeListener<Integer> listener ) {

            }
        };


        new HystrixPoolImpl( poolConfiguration );
    }


    @Test( expected = NullPointerException.class )
    public void invalidHosts() {
        final HystrixPoolConfiguration poolConfiguration = new HystrixPoolConfiguration() {

            @Override
            public String getServiceName() {
                return "test";
            }


            @Override
            public int getPort() {
                return 10000;
            }


            @Override
            public void addPortLister( final PropertyChangeListener<Integer> listener ) {

            }


            @Override
            public Collection<String> getSeedNodes() {
                return null;
            }


            @Override
            public void addSeedNodesLister( final PropertyChangeListener<Collection<String>> listener ) {

            }


            @Override
            public int getMaxConnectionsPerHost() {
                return 20;
            }


            @Override
            public void addMaxConnectionsLister( final PropertyChangeListener<Integer> listener ) {

            }


            @Override
            public int getDefaultConnectionsPerIdentity() {
                return 10;
            }


            @Override
            public void addDefaultConnectionsPerIdentity( final PropertyChangeListener<Integer> listener ) {

            }
        };


        new HystrixPoolImpl( poolConfiguration );
    }


    @Test( expected = IllegalStateException.class )
    public void closeTwice() {

        final HystrixPoolConfiguration poolConfiguration = new HystrixPoolConfiguration() {

            @Override
            public String getServiceName() {
                return "test";
            }


            @Override
            public int getPort() {
                return rule.getCassandraFig().getNativePort();
            }


            @Override
            public void addPortLister( final PropertyChangeListener<Integer> listener ) {

            }


            @Override
            public Collection<String> getSeedNodes() {
                return Collections.singleton( "localhost" );
            }


            @Override
            public void addSeedNodesLister( final PropertyChangeListener<Collection<String>> listener ) {

            }


            @Override
            public int getMaxConnectionsPerHost() {
                return 20;
            }


            @Override
            public void addMaxConnectionsLister( final PropertyChangeListener<Integer> listener ) {

            }


            @Override
            public int getDefaultConnectionsPerIdentity() {
                return 10;
            }


            @Override
            public void addDefaultConnectionsPerIdentity( final PropertyChangeListener<Integer> listener ) {

            }
        };


        HystrixPool pool = new HystrixPoolImpl( poolConfiguration );

        pool.close();

        pool.close();
    }


    @Test( expected = NullPointerException.class )
    public void sessionNull() {

        final HystrixPoolConfiguration poolConfiguration = new HystrixPoolConfiguration() {

            @Override
            public String getServiceName() {
                return "test";
            }


            @Override
            public int getPort() {
                return rule.getCassandraFig().getNativePort();
            }


            @Override
            public void addPortLister( final PropertyChangeListener<Integer> listener ) {

            }


            @Override
            public Collection<String> getSeedNodes() {
                return Collections.singleton( "localhost" );
            }


            @Override
            public void addSeedNodesLister( final PropertyChangeListener<Collection<String>> listener ) {

            }


            @Override
            public int getMaxConnectionsPerHost() {
                return 20;
            }


            @Override
            public void addMaxConnectionsLister( final PropertyChangeListener<Integer> listener ) {

            }



            @Override
            public int getDefaultConnectionsPerIdentity() {
                return 10;
            }


            @Override
            public void addDefaultConnectionsPerIdentity( final PropertyChangeListener<Integer> listener ) {

            }
        };


        HystrixPool pool = new HystrixPoolImpl( poolConfiguration );


        pool.getSession( null );
    }


    @Test( expected = IllegalArgumentException.class )
    public void sessionEmpty() {
        final HystrixPoolConfiguration poolConfiguration = new HystrixPoolConfiguration() {

            @Override
            public String getServiceName() {
                return "test";
            }


            @Override
            public int getPort() {
                return rule.getCassandraFig().getNativePort();
            }


            @Override
            public void addPortLister( final PropertyChangeListener<Integer> listener ) {

            }


            @Override
            public Collection<String> getSeedNodes() {
                return Collections.singleton( "localhost" );
            }


            @Override
            public void addSeedNodesLister( final PropertyChangeListener<Collection<String>> listener ) {

            }


            @Override
            public int getMaxConnectionsPerHost() {
                return 20;
            }


            @Override
            public void addMaxConnectionsLister( final PropertyChangeListener<Integer> listener ) {

            }




            @Override
            public int getDefaultConnectionsPerIdentity() {
                return 10;
            }


            @Override
            public void addDefaultConnectionsPerIdentity( final PropertyChangeListener<Integer> listener ) {

            }
        };


        HystrixPool pool = new HystrixPoolImpl( poolConfiguration );

        pool.getSession( "" );
    }
}
