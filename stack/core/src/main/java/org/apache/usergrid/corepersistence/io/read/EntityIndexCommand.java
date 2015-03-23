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
import java.util.Collections;
import java.util.List;

import org.apache.usergrid.corepersistence.util.CpNamingUtils;
import org.apache.usergrid.persistence.collection.CollectionScope;
import org.apache.usergrid.persistence.collection.EntityCollectionManager;
import org.apache.usergrid.persistence.collection.EntityCollectionManagerFactory;
import org.apache.usergrid.persistence.collection.EntitySet;
import org.apache.usergrid.persistence.collection.MvccEntity;
import org.apache.usergrid.persistence.collection.impl.CollectionScopeImpl;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.index.ApplicationEntityIndex;
import org.apache.usergrid.persistence.index.SearchTypes;
import org.apache.usergrid.persistence.index.impl.IndexScopeImpl;
import org.apache.usergrid.persistence.index.query.CandidateResult;
import org.apache.usergrid.persistence.index.query.CandidateResults;
import org.apache.usergrid.persistence.index.query.Query;
import org.apache.usergrid.persistence.model.entity.Entity;
import org.apache.usergrid.persistence.model.entity.Id;

import com.fasterxml.uuid.UUIDComparator;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import rx.Observable;
import rx.Subscriber;
import rx.functions.Func0;
import rx.functions.Func1;


@Singleton
public class EntityIndexCommand implements Command<Id, EntityIndexCommand.SearchResults> {

    private final Id applicationId;
    private final ApplicationScope applicationScope;
    private final ApplicationEntityIndex index;
    private final SearchTypes types;
    private final String query;
    private final int resultSetSize;
    private final String scopeType;
    private final EntityCollectionManagerFactory entityCollectionManagerFactory;


    @Inject
    public EntityIndexCommand( final Id applicationId, final ApplicationScope applicationScope,
                               final ApplicationEntityIndex index, final SearchTypes types, final String query,
                               final int resultSetSize, final String scopeType,
                               final EntityCollectionManagerFactory entityCollectionManagerFactory ) {
        this.applicationId = applicationId;
        this.applicationScope = applicationScope;

        this.index = index;
        this.types = types;
        this.query = query;
        this.resultSetSize = resultSetSize;
        this.scopeType = scopeType;
        this.entityCollectionManagerFactory = entityCollectionManagerFactory;
    }


    @Override
    public Observable<EntityIndexCommand.SearchResults> call( final Observable<Id> idObservable ) {

        //create our observable of candidate search results
        final Observable<CandidateResults> candidateResults = idObservable
            .flatMap( id -> Observable.create( new ElasticSearchObservable( initialSearch( id ), nextPage( id ) ) ) );

        final Observable<CandidateResult> candidateObservable =
            candidateResults.flatMap( candidates -> Observable.from( candidates ) );

        //since we'll only have at most 100 results in memory at a time, we roll up our groups and emit them on to
        // the collector
        final Observable<CandidateGroup> candidateGroup =
            candidateObservable.groupBy( candidate -> candidate.getId() ).map( observableGroup -> {


                //for each group, create a list, then sort by highest version first
                final List<CandidateResult> groupList = observableGroup.toList().toBlocking().last();

                Collections.sort( groupList, CandidateVersionComparator::compare );

                //create our candidate group and emit it
                final CandidateGroup group =
                    new CandidateGroup( groupList.get( 0 ), groupList.subList( 1, groupList.size() ) );

                return group;
            } );


        //buffer our candidate group up to our resultset size.
        final Observable<CandidateCollector> collectedCandidates =
            candidateGroup.buffer( resultSetSize ).flatMap( candidates -> {

                final Observable<CandidateCollector> collector = Observable.from( candidates ).collect(
                    () -> new CandidateCollector( resultSetSize ), ( candidateCollector, candidate ) -> {
                        //add our candidates to our collector
                        candidateCollector.addCandidate( candidate.toKeep );
                        //add our empty results
                        candidateCollector.addEmptyResults( candidate.toRemove );
                    } );

                return collector;
            } );

        //now we have our collected candidates, load them


        final Observable<SearchResults> loadedEntities = collectedCandidates.map( loadEntities(resultSetSize) );


        return loadedEntities;
    }


    /**
     * Perform the initial search with the sourceId
     */
    private Func0<CandidateResults> initialSearch( final Id sourceId ) {
        return () -> index.search( new IndexScopeImpl( sourceId, scopeType ), types, Query.fromQL( query ) );
    }


    /**
     * Search the next page for the specified source
     */
    private Func1<String, CandidateResults> nextPage( final Id sourceId ) {
        return cursor -> index
            .search( new IndexScopeImpl( sourceId, scopeType ), types, Query.fromQL( query ).withCursor( cursor ) );
    }


    /**
     * Function that will load our entities from our candidates, filter stale or missing candidates and return results
     * @param expectedSize
     * @return
     */
    private Func1<CandidateCollector, SearchResults> loadEntities(final int expectedSize) {
        return candidateCollector -> {

            //from our candidates, group them by id type so we can create scopes
            Observable.from( candidateCollector.getCandidates() ).groupBy( candidate -> candidate.getId().getType() )
                      .flatMap( groups -> {


                          final List<CandidateResult> candidates = groups.toList().toBlocking().last();

                          //we can get no results, so quit aggregating if there are none
                          if ( candidates.size() == 0 ) {
                              return Observable.just( new SearchResults( 0 ) );
                          }


                          final String typeName = candidates.get( 0 ).getId().getType();

                          final String collectionType = CpNamingUtils.getCollectionScopeNameFromEntityType( typeName );


                          //create our scope to get our entity manager

                          final CollectionScope scope =
                              new CollectionScopeImpl( applicationId, applicationId, collectionType );

                          final EntityCollectionManager ecm =
                              entityCollectionManagerFactory.createCollectionManager( scope );


                          //get our entity ids
                          final List<Id> entityIds =
                              Observable.from( candidates ).map( c -> c.getId() ).toList().toBlocking().last();

                          //TODO, change this out

                          //an observable of our entity set



                          //now go through all our candidates and verify

                          return Observable.from( candidates ).collect(  () -> new SearchResults( expectedSize ), (searchResults, candidate) ->{

                              final EntitySet entitySet = ecm.load( entityIds ).toBlocking().last();

                              final MvccEntity entity = entitySet.getEntity( candidate.getId() );


                              //our candidate it stale, or target entity was deleted add it to the remove of our collector
                              if(UUIDComparator.staticCompare( entity.getVersion(), candidate.getVersion()) > 0 || !entity.getEntity().isPresent()){
                                  searchResults.addToRemove( candidate );
                                  return;
                              }


                              searchResults.addEntity( entity.getEntity().get() );


                          } )
                              //add the existing set to remove to this set
                              .doOnNext( results -> results.addToRemove( candidateCollector.getToRemove() ) );

                      } );


            return null;
        };
    }




    /**
     * Collects all valid results (in order) as well as candidates to be removed
     */
    public static class CandidateCollector {
        private final List<CandidateResult> candidates;
        private final List<CandidateResult> toRemove;


        public CandidateCollector( final int maxSize ) {
            candidates = new ArrayList<>( maxSize );
            toRemove = new ArrayList<>( maxSize );
        }


        public void addCandidate( final CandidateResult candidate ) {
            this.candidates.add( candidate );
        }


        public void addEmptyResults( final Collection<CandidateResult> stale ) {
            this.toRemove.addAll( stale );
        }


        public List<CandidateResult> getCandidates() {
            return candidates;
        }


        public List<CandidateResult> getToRemove() {
            return toRemove;
        }
    }


    public static class SearchResults {
        private final List<Entity> entities;
        private final List<CandidateResult> toRemove;

        private String cursor;


        public SearchResults( final int maxSize ) {
            entities = new ArrayList<>( maxSize );
            this.toRemove = new ArrayList<>( maxSize );
        }


        public void addEntity( final Entity entity ) {
            this.entities.add( entity );
        }


        public void addToRemove( final Collection<CandidateResult> stale ) {
            this.toRemove.addAll( stale );
        }


        public void addToRemove( final CandidateResult candidateResult ) {
                   this.toRemove.add( candidateResult );
               }




        public void setCursor( final String cursor ) {
            this.cursor = cursor;
        }
    }


    /**
     * An observable that will perform a search and continually emit results while they exist.
     */
    public static class ElasticSearchObservable implements Observable.OnSubscribe<CandidateResults> {

        private final Func1<String, CandidateResults> fetchNextPage;
        private final Func0<CandidateResults> fetchInitialResults;


        public ElasticSearchObservable( final Func0<CandidateResults> fetchInitialResults,
                                        final Func1<String, CandidateResults> fetchNextPage ) {
            this.fetchInitialResults = fetchInitialResults;
            this.fetchNextPage = fetchNextPage;
        }


        @Override
        public void call( final Subscriber<? super CandidateResults> subscriber ) {

            subscriber.onStart();

            try {
                CandidateResults results = fetchInitialResults.call();


                //emit our next page
                while ( true ) {
                    subscriber.onNext( results );

                    //if we have no cursor, we're done
                    if ( !results.hasCursor() ) {
                        break;
                    }


                    //we have a cursor, get our results to emit for the next page
                    results = fetchNextPage.call( results.getCursor() );
                }

                subscriber.onCompleted();
            }
            catch ( Throwable t ) {
                subscriber.onError( t );
            }
        }
    }


    /**
     * A message that contains the candidate to keep, and the candidate toRemove
     */
    public static class CandidateGroup {
        private final CandidateResult toKeep;
        private final Collection<CandidateResult> toRemove;


        public CandidateGroup( final CandidateResult toKeep, final Collection<CandidateResult> toRemove ) {
            this.toKeep = toKeep;
            this.toRemove = toRemove;
        }
    }


    /**
     * Compares 2 candidates by version.  The max version is considered greater
     */
    private static final class CandidateVersionComparator {

        public static int compare( final CandidateResult o1, final CandidateResult o2 ) {
            return UUIDComparator.staticCompare( o1.getVersion(), o2.getVersion() );
        }
    }

    /***********************
     * FROM HERE DOWN IS EXPERIMENTAL
     *************************
     */


}
