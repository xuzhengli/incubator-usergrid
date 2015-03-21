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

package org.apache.usergrid.corepersistence.io.read;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.usergrid.persistence.index.ApplicationEntityIndex;
import org.apache.usergrid.persistence.index.IndexScope;
import org.apache.usergrid.persistence.index.SearchTypes;
import org.apache.usergrid.persistence.index.impl.IndexScopeImpl;
import org.apache.usergrid.persistence.index.query.CandidateResult;
import org.apache.usergrid.persistence.index.query.CandidateResults;
import org.apache.usergrid.persistence.index.query.Query;
import org.apache.usergrid.persistence.model.entity.Entity;
import org.apache.usergrid.persistence.model.entity.Id;

import rx.Observable;
import rx.functions.Action2;
import rx.functions.Func1;


public class EntityIndexCommands {


//    /**
//     * Perform a search of all the entities, and then return the observable of search results
//     * @param index
//     * @param edgeType
//     * @param types
//     * @param query
//     * @return
//     */
//    public static Func1<Id, SearchResults> searchEntities(final ApplicationEntityIndex index,final String edgeType, final SearchTypes types, final String query  ){
//
//        return nodeId -> {
//
//        }
//    }
    /**
     * Construct an indexScope from the input id type
     * @param type
     * @return
     */
   public static Func1<Id, IndexScope> createSearchScope(final String type){
       return id -> new IndexScopeImpl( id, type );
   }
    /**
     * Get our candidate results
     * @param index
     * @param types The types to return
     * @param query
     * @return
     */
    public static Func1<IndexScope, CandidateResults> getCandidates(final ApplicationEntityIndex index, final SearchTypes types, final String query){
        return indexScope -> index.search( indexScope, types, Query.fromQLNullSafe( query ) );
    }


    /**
     * Flattens candidate results into a single stream of a result
     * @return
     */
    public static Func1<CandidateResults, Observable<CandidateResult>> flattenCandidates(){
        return (CandidateResults candidateResults) -> Observable.from( candidateResults );
    }


    public static Action2<SearchResults, EntitySet> collectSet(){
        return (searchResults, entitySet) -> {
          searchResults.addEntities( entitySet.entities );
        };
    }



    public static class SearchResults{
        private final List<Entity> entities;
        private String cursor;


        public SearchResults(final int maxSize) {entities = new ArrayList<>(maxSize);}

        public void addEntities(final Collection<Entity> entities){
            this.entities.addAll( entities );

        }
        public void setCursor(final String cursor){
            this.cursor = cursor;
        }
    }

    public static class EntitySet{
        private final List<Entity> entities;


        public EntitySet( final List<Entity> entities ) {this.entities = entities;}
    }

}
