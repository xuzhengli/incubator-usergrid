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


import org.apache.usergrid.persistence.index.ApplicationEntityIndex;
import org.apache.usergrid.persistence.index.SearchTypes;
import org.apache.usergrid.persistence.index.query.CandidateResults;
import org.apache.usergrid.persistence.model.entity.Id;

import rx.Observable;

import static org.apache.usergrid.corepersistence.io.read.EntityIndexCommands.createSearchScope;
import static org.apache.usergrid.corepersistence.io.read.EntityIndexCommands.getCandidates;
import static org.apache.usergrid.persistence.core.util.IdGenerator.createId;


public class TempExample {

    //set our root observable


    public static void main(String[] args) {

        final Id rootId = createId( "thing" );

        final ApplicationEntityIndex index = null;


        final SearchTypes searchType = SearchTypes.fromTypes( "test" );

        final String query = "select * ";

        final Observable<CandidateResults> observable = Observable.just( rootId ).map( createSearchScope( "type" ) ).map(getCandidates(index, searchType, query));


        observable.doOnNext( a -> System.out.println( a) ).toBlocking().last();
    }


    private static final class ResultsCollector{

        /**
         * Add the candidates to our collection
         * @param results
         */
        public void addCandidates(final CandidateResults results ){

            //TODO, collect the results, removing groups
            Observable.from( results ).groupBy( candidate -> candidate.getId() ).collect(  )
        }

    }



}
