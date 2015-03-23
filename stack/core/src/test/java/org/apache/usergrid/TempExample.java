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

package org.apache.usergrid;


import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.usergrid.corepersistence.rx.impl.CollectUntil;
import org.apache.usergrid.persistence.index.ApplicationEntityIndex;
import org.apache.usergrid.persistence.index.SearchTypes;
import org.apache.usergrid.persistence.index.query.CandidateResult;
import org.apache.usergrid.persistence.index.query.CandidateResults;
import org.apache.usergrid.persistence.model.entity.Id;

import com.fasterxml.uuid.UUIDComparator;

import rx.Observable;
import rx.Statement;
import rx.Subscriber;
import rx.functions.Action2;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.observables.GroupedObservable;

import static org.apache.usergrid.corepersistence.io.read.EntityIndexCommands.createSearchScope;
import static org.apache.usergrid.corepersistence.io.read.EntityIndexCommands.getCandidates;
import static org.apache.usergrid.persistence.core.util.IdGenerator.createId;
import static org.junit.Assert.assertEquals;


public class TempExample {

    //set our root observable



//
//        final Id rootId = createId( "thing" );
//
//        final ApplicationEntityIndex index = null;
//
//
//        final SearchTypes searchType = SearchTypes.fromTypes( "test" );
//
//        final String query = "select * ";
//
//        final int selectSize = 100;
//
//        final Observable<CandidateResults> observable = Observable.just( rootId ).map( createSearchScope( "type" ) )
//                                                                  .map( getCandidates( index, searchType,
//                                                                      query ) ); //.compose();
//
//
//        final Observable<GroupedObservable<Id, CandidateResult>> next =
//            observable.flatMap( candidates -> Observable.from( candidates ) ).groupBy( result -> result.getId() );
//
//
//        final Observable<ResultsCollector> results = next.compose(  new CollectUntil<>(
//            //stop when the collector has a full set
//                    collector -> collector.hasFullSet(),
//            //create a new results collector, based on requested size
//            () -> new ResultsCollector( selectSize ),
//            //collect each candidate into our set of Candidate Results
//            ( collector, group ) -> {
//
//                    final List<CandidateResult> list = group.toList().toBlocking().last();
//
//                    collector.addCandidates( list );
//                } ) );
//

        //now reduce these results again via a cassandra collector



    @Test
    public void testLoops(){

       final Observable<Integer> observable =  Observable.create( new Observable.OnSubscribe<Integer>() {

            int value = 0;


            @Override
            public void call( final Subscriber<? super Integer> subscriber ) {
                subscriber.onNext( ++value );
            }
        } );




        final CounterCollector collector = new CounterCollector();

        final Action2<CounterCollector, Integer> collectorFunction  = (col, value) -> {col.set( value ); };



        final Func0<Boolean> complete =  () -> collector.isFull();


        final Observable<CounterCollector> collectorObservable = Statement.doWhile( observable,  complete ).collect( () -> collector, collectorFunction  );


        final int value = collectorObservable.toBlocking().last().lastValue;


        assertEquals(100, value);





    }


    private static class CounterCollector{
        private int lastValue;

        public void set(final int newValue){
            lastValue = newValue;
        }

        public boolean isFull(){
            return lastValue >= 100;
        }
    }


    private static final class ResultsCollector {

        private Map<Id, IndexResult<CandidateResult>> toKeep = new HashMap<>();
        private Set<CandidateResult> toRemove = new HashSet<>();

        private final int resultSetSize;


        private int currentIndex;


        private ResultsCollector( final int resultSetSize ) {this.resultSetSize = resultSetSize;}


        /**
         * Add the candidates to our collection
         */
        public void addCandidates( final List<CandidateResult> results ) {

            Collections.sort( results, CandidateVersionComparator::compare );

            final CandidateResult maxCandidate = results.get( 0 );

            //add everything we don't use to our toRemoveSet
            for ( int i = 1; i < results.size(); i++ ) {
                toRemove.add( results.get( i ) );
            }

            //we have this id already, remove it and pick the max
            final Id maxId = maxCandidate.getId();

            //see if it exists in our set to keep
            final IndexResult<CandidateResult> existingCandidate = toKeep.get( maxId );

            if ( existingCandidate != null ) {

                final CandidateResult existing = existingCandidate.value;

                //our new value is greater than our existing, replace it
                if ( CandidateVersionComparator.compare( maxCandidate, existing ) > 0 ) {
                    //add it to the keep
                    toKeep.put( maxId, new IndexResult( currentIndex, maxCandidate ) );

                    //remove the old value
                    toRemove.add( existingCandidate.value );
                }

                //what's in the map is already the max, add our candidate to the cleanup list
                else {
                    toRemove.add( maxCandidate );
                }
            }

            //add it to our list of items to keep
            else {
                toKeep.put( maxId, new IndexResult( currentIndex, maxCandidate ) );
            }


            //increment our index for the next invocation
            currentIndex++;
        }


        /**
         * We have a full set to evaluate for candidates
         */
        public boolean hasFullSet() {
            return resultSetSize >= toKeep.size();
        }


        /**
         * Get all the canidates we've collected
         */
        public Collection<IndexResult<CandidateResult>> getCandidates() {
            return toKeep.values();
        }

        public Collection<CandidateResult> getStaleCandidates(){ return toRemove;}
    }




    /**
     * Compares 2 candidates by version.  The max version is considered greater
     */
    private static final class CandidateVersionComparator {

        public static int compare( final CandidateResult o1, final CandidateResult o2 ) {
            return UUIDComparator.staticCompare( o1.getVersion(), o2.getVersion() );
        }
    }


    /**
     * A data message with an index and a result.  Can be used in aggregations
     * @param <T>
     */
    private static final class IndexResult<T>{
        private final int index;

        private final T value;


        private IndexResult( final int index, final T value ) {
            this.index = index;
            this.value = value;
        }
    }

}
