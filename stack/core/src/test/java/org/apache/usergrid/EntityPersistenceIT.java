/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.apache.usergrid;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.usergrid.persistence.EntityManager;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;

import static com.codahale.metrics.MetricRegistry.name;
import static org.junit.Assert.assertEquals;


@Ignore("Only used for monitoring output")
public class EntityPersistenceIT {

    @ClassRule
    public static CoreITSetup setup = new CoreITSetupImpl( ConcurrentCoreIteratorITSuite.cassandraResource );

    @Rule
    public CoreApplication app = new CoreApplication( setup );

    private static final Logger LOG = LoggerFactory.getLogger( EntityPersistenceIT.class );


    @Test
    public void runTest() throws ExecutionException, InterruptedException {

        final int count = 10000;

        final int workers = 4;

        final MetricRegistry metrics = new MetricRegistry();


        final Timer writeTimer = metrics.timer( name( EntityPersistenceIT.class, "writeTimes" ) );
        final Meter writeMeter = metrics.meter(name(EntityPersistenceIT.class, "writeSuccess"));
        final Meter errorMeter = metrics.meter(name(EntityPersistenceIT.class, "writeError"));

        //wire up the reporting
        final Slf4jReporter reporter = Slf4jReporter.forRegistry(metrics)
                                                    .outputTo( LOG )
                                                    .convertRatesTo( TimeUnit.SECONDS )
                                                    .convertDurationsTo( TimeUnit.MILLISECONDS )
                                                    .build();


        //report every 10 seconds
        reporter.start( 10, TimeUnit.SECONDS );

        final ExecutorService service = Executors.newFixedThreadPool( workers );

        List<Future<Integer>> futures = new ArrayList<>( workers );



        EntityManager em = app.getEm();

        for ( int i = 0; i < workers; i++ ) {
            Future<Integer> future = service.submit( new Writer(em, i, count, writeTimer, writeMeter, errorMeter) );

            futures.add( future );
        }


        int saved = 0;

        for(Future<Integer> future: futures){
            saved += future.get();
        }

        final int expectedSize = workers*count;


        //force one last output
        reporter.report();

        assertEquals( expectedSize, saved );
    }


    /**
     * Simple class that writes and takes metrics
     */
    private static class Writer implements Callable<Integer> {

        private final EntityManager entityManager;
        private final int workerId;
        private final int count;
        private final Timer timer;
        private final Meter writeMeter;
        private final Meter errorMeter;


        private Writer( final EntityManager entityManager, final int workerId, final int count, final Timer timer,
                        final Meter writeMeter, final Meter errorMeter ) {
            this.entityManager = entityManager;
            this.workerId = workerId;
            this.count = count;
            this.timer = timer;
            this.writeMeter = writeMeter;
            this.errorMeter = errorMeter;
        }


        @Override
        public Integer call() throws Exception {


            LOG.info( "Writing {} entities.", count );
            int created = 0;

            for ( int i = 0; i < count; i++ ) {
                String name = workerId + "-" + i;

                Map<String, Object> entity = new HashMap<String, Object>();

                entity.put( "name", name );
                entity.put( "index", i );
                entity.put( "worker", workerId );


                final Timer.Context context = timer.time();

                try {
                    entityManager.create( "test", entity );
                    context.stop();
                    writeMeter.mark();
                    created++;
                }
                catch ( Throwable t ) {
                    context.stop();
                    errorMeter.mark();
                    //we purposefully swallow, we don't want to throw this exception
                    LOG.error( "Unable to write entity in worker {} index {}", new Object[] { workerId, i, t } );
                }



            }

            return created;
        }


    }
}
