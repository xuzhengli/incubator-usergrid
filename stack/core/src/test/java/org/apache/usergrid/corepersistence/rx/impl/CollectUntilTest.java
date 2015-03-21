/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.usergrid.corepersistence.rx.impl;


import org.junit.Test;

import rx.Observable;

import static org.junit.Assert.assertEquals;


public class CollectUntilTest {

    @Test
    public void testCollectUntil() {

        final CollectUntil<Integer, CountCollector> collectUntil =
            new CollectUntil<>(
                () -> new CountCollector(),
                ( collector, value ) -> collector.mark(),
                collector -> collector.isFull() );


        final CountCollector collector = Observable.range( 0, 200 ).compose( collectUntil ).toBlocking().last();

        assertEquals( 100, collector.count );
    }


    private static final class CountCollector {

        private int count;


        public void mark() {
            count++;
        }


        public boolean isFull() {
            return count == 100;
        }
    }
}
