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

package org.apache.usergrid.persistence.core.rx;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.usergrid.persistence.core.hystrix.HystrixObservable;

import rx.Observable;
import rx.functions.Func1;
import rx.functions.FuncN;

import static org.junit.Assert.assertEquals;


/**
 * Tests that provides examples of how to perform more complex RX operations
 */
public class ParallelTest {

    private static final Logger logger = LoggerFactory.getLogger( ParallelTest.class );

//
//    private static final HystrixCommandGroupKey GROUP_KEY = HystrixCommandGroupKey.Factory.asKey( "TEST_KEY" );
//
//
//    public static final String THREAD_POOL_SIZE = CommandUtils.getThreadPoolCoreSize( GROUP_KEY.name() );
//
//    public static final String THREAD_POOL_QUEUE = CommandUtils.getThreadPoolMaxQueueSize( GROUP_KEY.name() );


    /**
     * An example of how an observable that requires a "fan out" then join should execute.
     */
//    @Test(timeout = 5000)
    @Test
    public void concurrentFunctions() {
        final String input = "input";

        final int size = 100;
        //since we start at index 0
        final int expected = size - 1;


        //set our size equal

        //latch used to make each thread block to prove correctness
        final CountDownLatch latch = new CountDownLatch( size );


        //create our observable and execute it in the I/O pool since we'll be doing I/O operations

        /**
         *  QUESTION: Should this use the computation scheduler since all operations (except the hystrix command) are
         *  non blocking?
         */

        final Observable<String> observable = HystrixObservable.user( input );


        Observable<Integer> thing = observable.flatMap( new Func1<String, Observable<Integer>>() {

            @Override
            public Observable<Integer> call( final String s ) {
                List<Observable<Integer>> functions = new ArrayList<Observable<Integer>>();

                logger.info( "Creating new set of observables in thread {}", 
                        Thread.currentThread().getName() );

                for ( int i = 0; i < size; i++ ) {


                    final int index = i;

                    // create a new observable and execute the function on it.  
                    // These should happen in parallel when a subscription occurs

                    /**
                     * QUESTION: Should this again be the process thread, not the I/O
                     */
                    Observable<String> newObservable = HystrixObservable.user( input );

                    Observable<Integer> transformed = newObservable.map( new Func1<String, Integer>() {

                        @Override
                        public Integer call( final String s ) {

                            final String threadName = Thread.currentThread().getName();

                            logger.info( "Invoking parallel task in thread {}", threadName );

                                    latch.countDown();

                                    try {
                                        latch.await();
                                    }
                                    catch ( InterruptedException e ) {
                                        throw new RuntimeException( "Interrupted", e );
                                    }

                            return index;
                        }
                    } );

                    functions.add( transformed );
                }

                /**
                 * Execute the functions above and zip the results together
                 */
                Observable<Integer> zipped = Observable.zip( functions, new FuncN<Integer>() {

                    @Override
                    public Integer call( final Object... args ) {

                        logger.info( "Invoking zip in thread {}", Thread.currentThread().getName() );

                        assertEquals( size, args.length );

                        for ( int i = 0; i < args.length; i++ ) {
                            assertEquals( "Indexes are returned in order", i, args[i] );
                        }

                        //just return our string
                        return ( Integer ) args[args.length - 1];
                    }
                } );

                return zipped;
            }
        } );


        final Integer last = thing.toBlocking().last();


        assertEquals( expected, last.intValue() );


    }
}
