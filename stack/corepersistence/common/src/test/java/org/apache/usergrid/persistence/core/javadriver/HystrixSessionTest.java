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


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.usergrid.persistence.core.cassandra.CassandraRule;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;


/**
 * Simple test for verifying our connectivity short circuits when our capacity is hit
 */
public class HystrixSessionTest {


    private static final Logger logger = LoggerFactory.getLogger( HystrixSessionTest.class );

    @ClassRule
    public static CassandraRule rule = new CassandraRule();


    private static final String keyspaceName = HystrixSessionTest.class.getSimpleName();
    private static final String tableName = keyspaceName + ".testdata";


    private static final int CONNECTIONS_PER_CUSTOMER = 12;


    private static final int CONNECTIONS_PER_HOST = 20;
    private HystrixPool pool;


    @Before
    public void setupPool() {
        final HystrixPoolConfiguration poolConfiguration = new HystrixPoolConfiguration() {

            @Override
            public String getServiceName() {
                return "testservice";
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
                return CONNECTIONS_PER_HOST;
            }


            @Override
            public void addMaxConnectionsLister( final PropertyChangeListener<Integer> listener ) {

            }


            @Override
            public int getDefaultConnectionsPerIdentity() {
                return CONNECTIONS_PER_CUSTOMER;
            }


            @Override
            public void addDefaultConnectionsPerIdentity( final PropertyChangeListener<Integer> listener ) {

            }
        };


        pool = new HystrixPoolImpl( poolConfiguration );

        final String keyspaceCreate =
                "CREATE KEYSPACE " + keyspaceName + " WITH REPLICATION = { 'class' : 'SimpleStrategy', "
                        + "'replication_factor' : 1 };";

        final String createColumnFamily = "CREATE TABLE " + tableName + " ( " +
                "entry_id uuid, " +
                "value bigint, " +
                "PRIMARY KEY (entry_id) " +
                "); ";


        Session session = pool.getSession( "setup" );

        try {

            session.execute( keyspaceCreate );
        }
        catch ( Exception e ) {

        }

        try {
            session.execute( createColumnFamily );
        }
        catch ( Exception e ) {

        }

        session.close();
    }


    @After
    public void closePool() {
        pool.close();
    }


    //    @Test( timeout = 5000 )
    @Test
    public void singleClient() throws Exception {


        ExecutorService executorService = Executors.newFixedThreadPool( 1 );


        Insert insert = new Insert( pool, "test" );

        Future<Void> future = executorService.submit( insert );

        insert.stop();

        future.get();

        executorService.shutdown();
    }


    @Test( timeout = 10000 )
    public void maxLimitSingleCustomer() throws Exception {
        /**
         * Note the way hystrix works, you can never hit exactly the limit.  You need to leave a bit of "headroom"
         * to stop execution from filling up
         */
        int numWorkers = ( int ) ( CONNECTIONS_PER_CUSTOMER * .6 );
        InsertCoordinator insertCoordinator = new InsertCoordinator( pool, numWorkers, "customer" );
        insertCoordinator.start();

        Thread.sleep( 2000 );

        insertCoordinator.stop();
    }


    @Test( timeout = 10000, expected = ExecutionException.class )
    public void overMaxLimitSingleCustomer() throws Exception {
        InsertCoordinator insertCoordinator = new InsertCoordinator( pool, CONNECTIONS_PER_CUSTOMER, "customer2" );
        insertCoordinator.start();

        Thread.sleep( 2000 );


        insertCoordinator.stop();
    }


    /**
     * Coordinates multiple groups on inset
     */
    private static class InsertCoordinator {

        private final List<InsertGroup> insertGroups;


        /**
         * Create an insert coordinator
         *
         * @param pool The customer pool
         * @param size The size of concurrent inserts per identifier
         * @param identifiers The
         */
        public InsertCoordinator( final HystrixPool pool, int size, String... identifiers )
                throws InterruptedException {

            insertGroups = new ArrayList<InsertGroup>();

            for ( String identifier : identifiers ) {
                InsertGroup group = new InsertGroup( pool, identifier, size );

                insertGroups.add( group );
            }
        }


        /**
         * Start all insert groups running
         */
        public void start() {
            for ( InsertGroup group : insertGroups ) {
                group.start();
            }
        }


        /**
         * Stop the group
         */
        public void stop() throws ExecutionException, InterruptedException {
            for ( InsertGroup group : insertGroups ) {
                group.stop();
            }
        }
    }


    /**
     * Represents a group of insert workers
     */
    private static class InsertGroup {

        private final int size;

        private final String identifier;

        private final HystrixPool pool;

        private final List<Insert> inserts;

        private final List<Future<Void>> futures;

        private final ExecutorService executorService;


        /**
         * Create an insert group
         *
         * @param pool The customer pool
         * @param identifier The identifier to use when getting a session from the pool
         * @param size The size of concurrent inserts to create using the identifier
         */
        private InsertGroup( final HystrixPool pool, final String identifier, final int size ) {
            this.pool = pool;
            this.size = size;
            this.identifier = identifier;
            this.inserts = new ArrayList<Insert>( size );
            this.futures = new ArrayList<Future<Void>>( size );
            this.executorService = Executors.newFixedThreadPool( size );
        }


        /**
         * Start the workers
         */
        public void start() {

            for ( int i = 0; i < size; i++ ) {
                Insert insert = new Insert( pool, identifier );

                inserts.add( insert );

                Future<Void> future = executorService.submit( insert );

                futures.add( future );
            }
        }


        /**
         * Stop the workers of this insert service
         */
        public void stop() throws ExecutionException, InterruptedException {
            for ( Insert insert : inserts ) {
                insert.stop();
            }

            /**
             * Now get all futures to see if any exceptions where thrown
             *
             */

            for ( Future<Void> future : futures ) {
                future.get();
            }

            executorService.shutdown();
        }
    }


    private static class Insert implements Callable<Void> {

        private static final long VALUE = 1000l;

        private final Session session;
        private final PreparedStatement preparedStatement;
        private volatile boolean running = true;


        public Insert( final HystrixPool pool, final String identifier ) {
            this.session = pool.getSession( identifier );
            this.preparedStatement = session.prepare( "INSERT INTO " + tableName + " (entry_id, value) values (?, ?)" );
        }


        @Override
        public Void call() throws Exception {

            logger.info( "Starting write on worker {}", Thread.currentThread().getName() );

            while ( running ) {
                final UUID entryId = UUID.randomUUID();

                BoundStatement boundStatement = preparedStatement.bind( entryId, VALUE );

                /**
                 * Await for everyone to arrive so we execute the session at the same time
                 */

                logger.debug( "Waiting for concurrent write" );


                logger.debug( "Performing write" );
                session.execute( boundStatement );
            }

            logger.info( "Completed write" );

            return null;
        }


        public void stop() {
            this.running = false;
        }
    }
}


