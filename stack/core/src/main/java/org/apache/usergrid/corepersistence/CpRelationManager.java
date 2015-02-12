/*
 * Copyright 2014 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.usergrid.corepersistence;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import org.apache.usergrid.corepersistence.results.ResultsLoaderFactory;
import org.apache.usergrid.corepersistence.results.ResultsLoaderFactoryImpl;
import org.apache.usergrid.corepersistence.util.CpEntityMapUtils;
import org.apache.usergrid.corepersistence.util.CpNamingUtils;
import org.apache.usergrid.persistence.ConnectedEntityRef;
import org.apache.usergrid.persistence.ConnectionRef;
import org.apache.usergrid.persistence.Entity;
import org.apache.usergrid.persistence.EntityManager;
import org.apache.usergrid.persistence.EntityRef;
import org.apache.usergrid.persistence.IndexBucketLocator;
import org.apache.usergrid.persistence.RelationManager;
import org.apache.usergrid.persistence.Results;
import org.apache.usergrid.persistence.RoleRef;
import org.apache.usergrid.persistence.Schema;
import org.apache.usergrid.persistence.SimpleEntityRef;
import org.apache.usergrid.persistence.SimpleRoleRef;
import org.apache.usergrid.persistence.cassandra.CassandraService;
import org.apache.usergrid.persistence.cassandra.ConnectionRefImpl;
import org.apache.usergrid.persistence.collection.CollectionScope;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.entities.Group;
import org.apache.usergrid.persistence.entities.User;
import org.apache.usergrid.persistence.graph.Edge;
import org.apache.usergrid.persistence.graph.GraphManager;
import org.apache.usergrid.persistence.graph.SearchByEdgeType;
import org.apache.usergrid.persistence.graph.impl.SimpleEdge;
import org.apache.usergrid.persistence.graph.impl.SimpleSearchByEdge;
import org.apache.usergrid.persistence.graph.impl.SimpleSearchByEdgeType;
import org.apache.usergrid.persistence.graph.impl.SimpleSearchEdgeType;
import org.apache.usergrid.persistence.index.EntityIndex;
import org.apache.usergrid.persistence.index.EntityIndexBatch;
import org.apache.usergrid.persistence.index.IndexScope;
import org.apache.usergrid.persistence.index.SearchTypes;
import org.apache.usergrid.persistence.index.impl.IndexScopeImpl;
import org.apache.usergrid.persistence.index.query.CandidateResult;
import org.apache.usergrid.persistence.index.query.CandidateResults;
import org.apache.usergrid.persistence.index.query.Identifier;
import org.apache.usergrid.persistence.index.query.Query;
import org.apache.usergrid.persistence.index.query.Query.Level;
import org.apache.usergrid.persistence.model.entity.Id;
import org.apache.usergrid.persistence.model.entity.SimpleId;
import org.apache.usergrid.persistence.schema.CollectionInfo;
import org.apache.usergrid.utils.MapUtils;
import org.apache.usergrid.utils.UUIDUtils;

import com.google.common.base.Preconditions;
import com.yammer.metrics.annotation.Metered;

import me.prettyprint.hector.api.Keyspace;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import static org.apache.usergrid.corepersistence.util.CpNamingUtils.getCollectionScopeNameFromEntityType;
import static org.apache.usergrid.persistence.Schema.COLLECTION_ROLES;
import static org.apache.usergrid.persistence.Schema.PROPERTY_CREATED;
import static org.apache.usergrid.persistence.Schema.PROPERTY_INACTIVITY;
import static org.apache.usergrid.persistence.Schema.PROPERTY_NAME;
import static org.apache.usergrid.persistence.Schema.PROPERTY_TITLE;
import static org.apache.usergrid.persistence.Schema.TYPE_ENTITY;
import static org.apache.usergrid.persistence.Schema.TYPE_ROLE;
import static org.apache.usergrid.persistence.Schema.getDefaultSchema;
import static org.apache.usergrid.utils.ClassUtils.cast;
import static org.apache.usergrid.utils.InflectionUtils.singularize;
import static org.apache.usergrid.utils.MapUtils.addMapSet;


/**
 * Implement good-old Usergrid RelationManager with the new-fangled Core Persistence API.
 */
public class CpRelationManager implements RelationManager {

    private static final Logger logger = LoggerFactory.getLogger( CpRelationManager.class );


    private CpEntityManagerFactory emf;

    private ManagerCache managerCache;

    private EntityManager em;

    private UUID applicationId;

    private EntityRef headEntity;

    private org.apache.usergrid.persistence.model.entity.Entity cpHeadEntity;

    private ApplicationScope applicationScope;
    private CollectionScope headEntityScope;

    private CassandraService cass;

    private IndexBucketLocator indexBucketLocator;

    private ResultsLoaderFactory resultsLoaderFactory;



    public CpRelationManager() {}


    public CpRelationManager init(
        EntityManager em,
            CpEntityManagerFactory emf,
            UUID applicationId,
            EntityRef headEntity,
            IndexBucketLocator indexBucketLocator ) {

        Assert.notNull( em, "Entity manager cannot be null" );
        Assert.notNull( emf, "Entity manager factory cannot be null" );
        Assert.notNull( applicationId, "Application Id cannot be null" );
        Assert.notNull( headEntity, "Head entity cannot be null" );
        Assert.notNull( headEntity.getUuid(), "Head entity uuid cannot be null" );

        // TODO: this assert should not be failing
        //Assert.notNull( indexBucketLocator, "indexBucketLocator cannot be null" );

        this.em = em;
        this.emf = emf;
        this.applicationId = applicationId;
        this.headEntity = headEntity;
        this.managerCache = emf.getManagerCache();
        this.applicationScope = CpNamingUtils.getApplicationScope( applicationId );

        this.cass = em.getCass(); // TODO: eliminate need for this via Core Persistence
        this.indexBucketLocator = indexBucketLocator; // TODO: this also

        // load the Core Persistence version of the head entity as well
        this.headEntityScope = getCollectionScopeNameFromEntityType(
                applicationScope.getApplication(), headEntity.getType());

        if ( logger.isDebugEnabled() ) {
            logger.debug( "Loading head entity {}:{} from scope\n   app {}\n   owner {}\n   name {}",
                new Object[] {
                    headEntity.getType(),
                    headEntity.getUuid(),
                    headEntityScope.getApplication(),
                    headEntityScope.getOwner(),
                    headEntityScope.getName()
                } );
        }

        Id entityId = new SimpleId( headEntity.getUuid(), headEntity.getType() );
        this.cpHeadEntity = ((CpEntityManager)em).load(
            new CpEntityManager.EntityScope( headEntityScope, entityId));

        // commented out because it is possible that CP entity has not been created yet
        Assert.notNull( cpHeadEntity, "cpHeadEntity cannot be null" );

        this.resultsLoaderFactory = new ResultsLoaderFactoryImpl( managerCache );

        return this;
    }


    @Override
    public Set<String> getCollectionIndexes( String collectionName ) throws Exception {
        final Set<String> indexes = new HashSet<String>();

        GraphManager gm = managerCache.getGraphManager(applicationScope);

        String edgeTypePrefix = CpNamingUtils.getEdgeTypeFromCollectionName( collectionName );

        logger.debug("getCollectionIndexes(): Searching for edge type prefix {} to target {}:{}",
            new Object[] {
                edgeTypePrefix, cpHeadEntity.getId().getType(), cpHeadEntity.getId().getUuid()
        });

        Observable<String> types= gm.getEdgeTypesFromSource(
            new SimpleSearchEdgeType( cpHeadEntity.getId(), edgeTypePrefix,  null ));

        Iterator<String> iter = types.toBlockingObservable().getIterator();
        while ( iter.hasNext() ) {
            indexes.add( iter.next() );
        }
        return indexes;
    }


    @Override
    public Map<String, Map<UUID, Set<String>>> getOwners() throws Exception {

        // TODO: do we need to restrict this to edges prefixed with owns?
        //Map<EntityRef, Set<String>> containerEntities = getContainers(-1, "owns", null);
        Map<EntityRef, Set<String>> containerEntities = getContainers();

        Map<String, Map<UUID, Set<String>>> owners =
                new LinkedHashMap<String, Map<UUID, Set<String>>>();

        for ( EntityRef owner : containerEntities.keySet() ) {
            Set<String> collections = containerEntities.get( owner );
            for ( String collection : collections ) {
                MapUtils.addMapMapSet( owners, owner.getType(), owner.getUuid(), collection );
            }
        }

        return owners;
    }


    private Map<EntityRef, Set<String>> getContainers() {
        return getContainers( -1, null, null );
    }


    /**
     * Gets containing collections and/or connections depending on the edge type you pass in
     * @param limit Max number to return
     * @param edgeType Edge type, edge type prefix or null to allow any edge type
     * @param fromEntityType Only consider edges from entities of this type
     */
    Map<EntityRef, Set<String>> getContainers( int limit, String edgeType, String fromEntityType ) {

        Map<EntityRef, Set<String>> results = new LinkedHashMap<EntityRef, Set<String>>();

        GraphManager gm = managerCache.getGraphManager(applicationScope);

        Iterator<String> edgeTypes = gm.getEdgeTypesToTarget( new SimpleSearchEdgeType(
            cpHeadEntity.getId(), edgeType, null) ).toBlocking().getIterator();

        logger.debug("getContainers(): "
                + "Searched for edges of type {}\n   to target {}:{}\n   in scope {}\n   found: {}",
            new Object[] {
                edgeType,
                cpHeadEntity.getId().getType(),
                cpHeadEntity.getId().getUuid(),
                applicationScope.getApplication(),
                edgeTypes.hasNext()
        });

        while ( edgeTypes.hasNext() ) {

            String etype = edgeTypes.next();

            Observable<Edge> edges = gm.loadEdgesToTarget( new SimpleSearchByEdgeType(
                cpHeadEntity.getId(), etype, Long.MAX_VALUE, SearchByEdgeType.Order.DESCENDING, null ));

            Iterator<Edge> iter = edges.toBlockingObservable().getIterator();
            while ( iter.hasNext() ) {
                Edge edge = iter.next();

                if ( fromEntityType != null && !fromEntityType.equals( edge.getSourceNode().getType() )) {
                    logger.debug("Ignoring edge from entity type {}", edge.getSourceNode().getType());
                    continue;
                }

                EntityRef eref = new SimpleEntityRef(
                        edge.getSourceNode().getType(), edge.getSourceNode().getUuid() );

                String name = null;
                if ( CpNamingUtils.isConnectionEdgeType( edge.getType() )) {
                    name = CpNamingUtils.getConnectionType( edge.getType() );
                } else {
                    name = CpNamingUtils.getCollectionName( edge.getType() );
                }
                addMapSet( results, eref, name );
            }

            if ( limit > 0 && results.keySet().size() >= limit ) {
                break;
            }
        }

        return results;
    }


    public void updateContainingCollectionAndCollectionIndexes(
            final org.apache.usergrid.persistence.model.entity.Entity cpEntity ) {


        final GraphManager gm = managerCache.getGraphManager( applicationScope );

        Iterator<String> edgeTypesToTarget = gm.getEdgeTypesToTarget( new SimpleSearchEdgeType(
            cpHeadEntity.getId(), null, null) ).toBlockingObservable().getIterator();

        logger.debug("updateContainingCollectionsAndCollections(): "
                + "Searched for edges to target {}:{}\n   in scope {}\n   found: {}",
            new Object[] {
                cpHeadEntity.getId().getType(),
                cpHeadEntity.getId().getUuid(),
                applicationScope.getApplication(),
                edgeTypesToTarget.hasNext()
        });

        // loop through all types of edge to target


        final EntityIndex ei = managerCache.getEntityIndex( applicationScope );

        final EntityIndexBatch entityIndexBatch = ei.createBatch();

        final int count = gm.getEdgeTypesToTarget(
            new SimpleSearchEdgeType( cpHeadEntity.getId(), null, null ) )

                // for each edge type, emit all the edges of that type
                .flatMap( new Func1<String, Observable<Edge>>() {
                    @Override
                    public Observable<Edge> call( final String etype ) {
                        return gm.loadEdgesToTarget( new SimpleSearchByEdgeType(
                            cpHeadEntity.getId(), etype, Long.MAX_VALUE,
                            SearchByEdgeType.Order.DESCENDING, null ) );
                    }
                } )

                //for each edge we receive index and add to the batch
                .doOnNext( new Action1<Edge>() {
                    @Override
                    public void call( final Edge edge ) {

                        EntityRef sourceEntity =
                                new SimpleEntityRef( edge.getSourceNode().getType(), edge.getSourceNode().getUuid() );

                        // reindex the entity in the source entity's collection or connection index

                        IndexScope indexScope;
                        if ( CpNamingUtils.isCollectionEdgeType( edge.getType() ) ) {

                            String collName = CpNamingUtils.getCollectionName( edge.getType() );
                            indexScope = new IndexScopeImpl(
                                new SimpleId( sourceEntity.getUuid(), sourceEntity.getType()),
                                CpNamingUtils.getCollectionScopeNameFromCollectionName( collName ));
                        }
                        else {

                            String connName = CpNamingUtils.getConnectionType( edge.getType() );
                            indexScope = new IndexScopeImpl(
                                new SimpleId( sourceEntity.getUuid(), sourceEntity.getType() ),
                                CpNamingUtils.getConnectionScopeName( connName ) );
                        }

                        entityIndexBatch.index( indexScope, cpEntity );

                        // reindex the entity in the source entity's all-types index

                        //TODO REMOVE INDEX CODE
                        //                        indexScope = new IndexScopeImpl( new SimpleId(
                        //                            sourceEntity.getUuid(), sourceEntity.getType() ), CpNamingUtils
                        // .ALL_TYPES, entityType );
                        //
                        //                        entityIndexBatch.index( indexScope, cpEntity );
                    }
                } ).count().toBlocking().lastOrDefault( 0 );


        entityIndexBatch.execute();

        logger.debug( "updateContainingCollectionsAndCollections() updated {} indexes", count );
    }


    @Override
    public boolean isConnectionMember( String connectionType, EntityRef entity ) throws Exception {

        Id entityId = new SimpleId( entity.getUuid(), entity.getType() );

        String edgeType = CpNamingUtils.getEdgeTypeFromConnectionType( connectionType );

        logger.debug("isConnectionMember(): Checking for edge type {} from {}:{} to {}:{}",
            new Object[] {
                edgeType,
                headEntity.getType(), headEntity.getUuid(),
                entity.getType(), entity.getUuid() });

        GraphManager gm = managerCache.getGraphManager( applicationScope );
        Observable<Edge> edges = gm.loadEdgeVersions( new SimpleSearchByEdge(
            new SimpleId( headEntity.getUuid(), headEntity.getType() ),
            edgeType,
            entityId,
            Long.MAX_VALUE,
            SearchByEdgeType.Order.DESCENDING,
            null ) );

        return edges.toBlockingObservable().firstOrDefault( null ) != null;
    }


    @SuppressWarnings( "unchecked" )
    @Metered( group = "core", name = "RelationManager_isOwner" )
    @Override
    public boolean isCollectionMember( String collName, EntityRef entity ) throws Exception {

        Id entityId = new SimpleId( entity.getUuid(), entity.getType() );

        String edgeType = CpNamingUtils.getEdgeTypeFromCollectionName( collName );

        logger.debug("isCollectionMember(): Checking for edge type {} from {}:{} to {}:{}",
            new Object[] {
                edgeType,
                headEntity.getType(), headEntity.getUuid(),
                entity.getType(), entity.getUuid() });

        GraphManager gm = managerCache.getGraphManager( applicationScope );
        Observable<Edge> edges = gm.loadEdgeVersions( new SimpleSearchByEdge(
            new SimpleId( headEntity.getUuid(), headEntity.getType() ),
            edgeType,
            entityId,
            Long.MAX_VALUE,
            SearchByEdgeType.Order.DESCENDING,
            null ) );

        return edges.toBlockingObservable().firstOrDefault( null ) != null;
    }


    private boolean moreThanOneInboundConnection( EntityRef target, String connectionType ) {

        Id targetId = new SimpleId( target.getUuid(), target.getType() );

        GraphManager gm = managerCache.getGraphManager( applicationScope );

        Observable<Edge> edgesToTarget = gm.loadEdgesToTarget( new SimpleSearchByEdgeType(
            targetId,
            CpNamingUtils.getEdgeTypeFromConnectionType( connectionType ),
            System.currentTimeMillis(),
            SearchByEdgeType.Order.DESCENDING,
            null ) ); // last

        Iterator<Edge> iterator = edgesToTarget.toBlockingObservable().getIterator();
        int count = 0;
        while ( iterator.hasNext() ) {
            iterator.next();
            if ( count++ > 1 ) {
                return true;
            }
        }
        return false;
    }


    private boolean moreThanOneOutboundConnection( EntityRef source, String connectionType ) {

        Id sourceId = new SimpleId( source.getUuid(), source.getType() );

        GraphManager gm = managerCache.getGraphManager( applicationScope );

        Observable<Edge> edgesFromSource = gm.loadEdgesFromSource( new SimpleSearchByEdgeType(
            sourceId,
            CpNamingUtils.getEdgeTypeFromConnectionType( connectionType ),
            System.currentTimeMillis(),
            SearchByEdgeType.Order.DESCENDING,
            null ) ); // last

        int count = edgesFromSource.take( 2 ).count().toBlocking().last();

        return count > 1;
    }


    @Override
    public Set<String> getCollections() throws Exception {

        final Set<String> indexes = new HashSet<String>();

        GraphManager gm = managerCache.getGraphManager( applicationScope );

        Observable<String> str = gm.getEdgeTypesFromSource(
                new SimpleSearchEdgeType( cpHeadEntity.getId(), null, null ) );

        Iterator<String> iter = str.toBlockingObservable().getIterator();
        while ( iter.hasNext() ) {
            String edgeType = iter.next();
            indexes.add( CpNamingUtils.getCollectionName( edgeType ) );
        }

        return indexes;
    }


    @Override
    public Results getCollection( String collectionName,
            UUID startResult,
            int count,
            Level resultsLevel,
            boolean reversed ) throws Exception {

        Query query = Query.fromQL( "select *" );
        query.setLimit( count );
        query.setReversed( reversed );

        if ( startResult != null ) {
            query.addGreaterThanEqualFilter( "created", startResult.timestamp() );
        }

        return searchCollection( collectionName, query );
    }


    @Override
    public Results getCollection( String collName, Query query, Level level ) throws Exception {

        return searchCollection( collName, query );
    }


    // add to a named collection of the head entity
    @Override
    public Entity addToCollection( String collName, EntityRef itemRef ) throws Exception {

        CollectionInfo collection =
                getDefaultSchema().getCollection( headEntity.getType(), collName );
        if ( ( collection != null ) && !collection.getType().equals( itemRef.getType() ) ) {
            return null;
        }

        return addToCollection( collName, itemRef,
                ( collection != null && collection.getLinkedCollection() != null ) );
    }


    public Entity addToCollection( String collName, EntityRef itemRef, boolean connectBack )
            throws Exception {

        CollectionScope memberScope = getCollectionScopeNameFromEntityType(
                applicationScope.getApplication(), itemRef.getType());

        Id entityId = new SimpleId( itemRef.getUuid(), itemRef.getType() );
        org.apache.usergrid.persistence.model.entity.Entity memberEntity =
            ((CpEntityManager)em).load( new CpEntityManager.EntityScope( memberScope, entityId));

        return addToCollection(collName, itemRef, memberEntity, connectBack);
    }


    public Entity addToCollection(final String collName, final EntityRef itemRef,
            final org.apache.usergrid.persistence.model.entity.Entity memberEntity, final boolean connectBack )
        throws Exception {

        // don't fetch entity if we've already got one
        final Entity itemEntity;
        if ( itemRef instanceof Entity ) {
            itemEntity = ( Entity ) itemRef;
        }
        else {
            itemEntity = em.get( itemRef );
        }

        if ( itemEntity == null ) {
            return null;
        }

        CollectionInfo collection = getDefaultSchema().getCollection( headEntity.getType(), collName );
        if ( ( collection != null ) && !collection.getType().equals( itemRef.getType() ) ) {
            return null;
        }

        // load the new member entity to be added to the collection from its default scope
        CollectionScope memberScope = getCollectionScopeNameFromEntityType(
                applicationScope.getApplication(), itemRef.getType());

        //TODO, this double load should disappear once events are in
        Id entityId = new SimpleId( itemRef.getUuid(), itemRef.getType() );

        if ( memberEntity == null ) {
            throw new RuntimeException(
                    "Unable to load entity uuid=" + itemRef.getUuid() + " type=" + itemRef.getType() );
        }

        if ( logger.isDebugEnabled() ) {
            logger.debug( "Loaded member entity {}:{} from scope\n   app {}\n   "
                + "owner {}\n   name {} data {}",
                new Object[] {
                    itemRef.getType(),
                    itemRef.getUuid(),
                    memberScope.getApplication(),
                    memberScope.getOwner(),
                    memberScope.getName(),
                    CpEntityMapUtils.toMap( memberEntity )
                } );
        }

        String edgeType = CpNamingUtils.getEdgeTypeFromCollectionName( collName );

        UUID timeStampUuid = memberEntity.getId().getUuid() != null
                && UUIDUtils.isTimeBased( memberEntity.getId().getUuid() )
                ?  memberEntity.getId().getUuid() : UUIDUtils.newTimeUUID();

        long uuidHash = UUIDUtils.getUUIDLong( timeStampUuid );

        // create graph edge connection from head entity to member entity
        Edge edge = new SimpleEdge( cpHeadEntity.getId(), edgeType, memberEntity.getId(), uuidHash );
        GraphManager gm = managerCache.getGraphManager( applicationScope );
        gm.writeEdge( edge ).toBlockingObservable().last();

        logger.debug( "Wrote edgeType {}\n   from {}:{}\n   to {}:{}\n   scope {}:{}",
            new Object[] {
                edgeType,
                cpHeadEntity.getId().getType(),
                cpHeadEntity.getId().getUuid(),
                memberEntity.getId().getType(),
                memberEntity.getId().getUuid(),
                applicationScope.getApplication().getType(),
                applicationScope.getApplication().getUuid()
        } );

        ( ( CpEntityManager ) em ).indexEntityIntoCollection( cpHeadEntity, memberEntity, collName );

        logger.debug( "Added entity {}:{} to collection {}", new Object[] {
                itemRef.getUuid().toString(), itemRef.getType(), collName
        } );

        //        logger.debug("With head entity scope is {}:{}:{}", new Object[] {
        //            headEntityScope.getApplication().toString(),
        //            headEntityScope.getOwner().toString(),
        //            headEntityScope.getName()});

        if ( connectBack && collection != null && collection.getLinkedCollection() != null ) {
            getRelationManager( itemEntity ).addToCollection(
                    collection.getLinkedCollection(), headEntity, cpHeadEntity, false );
            getRelationManager( itemEntity ).addToCollection(
                    collection.getLinkedCollection(), headEntity, false );
        }

        return itemEntity;
    }


    @Override
    public Entity addToCollections( List<EntityRef> owners, String collName ) throws Exception {

        // TODO: this addToCollections() implementation seems wrong.
        for ( EntityRef eref : owners ) {
            addToCollection( collName, eref );
        }

        return null;
    }


    @Override
    @Metered( group = "core", name = "RelationManager_createItemInCollection" )
    public Entity createItemInCollection(
        String collName, String itemType, Map<String, Object> properties) throws Exception {

        if ( headEntity.getUuid().equals( applicationId ) ) {
            if ( itemType.equals( TYPE_ENTITY ) ) {
                itemType = singularize( collName );
            }

            if ( itemType.equals( TYPE_ROLE ) ) {
                Long inactivity = ( Long ) properties.get( PROPERTY_INACTIVITY );
                if ( inactivity == null ) {
                    inactivity = 0L;
                }
                return em.createRole( ( String ) properties.get( PROPERTY_NAME ),
                        ( String ) properties.get( PROPERTY_TITLE ), inactivity );
            }
            return em.create( itemType, properties );
        }

        else if ( headEntity.getType().equals( Group.ENTITY_TYPE )
                && ( collName.equals( COLLECTION_ROLES ) ) ) {
            UUID groupId = headEntity.getUuid();
            String roleName = ( String ) properties.get( PROPERTY_NAME );
            return em.createGroupRole( groupId, roleName, ( Long ) properties.get( PROPERTY_INACTIVITY ) );
        }

        CollectionInfo collection = getDefaultSchema().getCollection( headEntity.getType(), collName );
        if ( ( collection != null ) && !collection.getType().equals( itemType ) ) {
            return null;
        }

        properties = getDefaultSchema().cleanUpdatedProperties( itemType, properties, true );

        Entity itemEntity = em.create( itemType, properties );

        if ( itemEntity != null ) {

            addToCollection( collName, itemEntity );

            if ( collection != null && collection.getLinkedCollection() != null ) {
                getRelationManager( getHeadEntity() )
                        .addToCollection( collection.getLinkedCollection(), itemEntity );
            }
        }

        return itemEntity;
    }


    @Override
    public void removeFromCollection( String collName, EntityRef itemRef ) throws Exception {

        // special handling for roles collection of the application
        if ( headEntity.getUuid().equals( applicationId ) ) {
            if ( collName.equals( COLLECTION_ROLES ) ) {
                Entity itemEntity = em.get( itemRef );
                if ( itemEntity != null ) {
                    RoleRef roleRef = SimpleRoleRef.forRoleEntity( itemEntity );
                    em.deleteRole( roleRef.getApplicationRoleName() );
                    return;
                }
                em.delete( itemEntity );
                return;
            }
            em.delete( itemRef );
            return;
        }

        // load the entity to be removed to the collection
        CollectionScope memberScope = getCollectionScopeNameFromEntityType(
                applicationScope.getApplication(), itemRef.getType());

        if ( logger.isDebugEnabled() ) {
            logger.debug( "Loading entity to remove from collection "
                + "{}:{} from scope\n   app {}\n   owner {}\n   name {}",
                new Object[] {
                    itemRef.getType(),
                    itemRef.getUuid(),
                    memberScope.getApplication(),
                    memberScope.getOwner(),
                    memberScope.getName()
               });
        }

        Id entityId = new SimpleId( itemRef.getUuid(), itemRef.getType() );
        org.apache.usergrid.persistence.model.entity.Entity memberEntity =
            ((CpEntityManager)em).load( new CpEntityManager.EntityScope( memberScope, entityId));

        final EntityIndex ei = managerCache.getEntityIndex( applicationScope );
        final EntityIndexBatch batch = ei.createBatch();

        // remove item from collection index
        IndexScope indexScope = new IndexScopeImpl(
            cpHeadEntity.getId(),
            CpNamingUtils.getCollectionScopeNameFromCollectionName( collName ) );

        batch.deindex( indexScope, memberEntity );

        // remove collection from item index
        IndexScope itemScope = new IndexScopeImpl(
            memberEntity.getId(),
            CpNamingUtils.getCollectionScopeNameFromCollectionName(
                    Schema.defaultCollectionName( cpHeadEntity.getId().getType() ) ) );


        batch.deindex( itemScope, cpHeadEntity );

        batch.execute();

        // remove edge from collection to item
        GraphManager gm = managerCache.getGraphManager( applicationScope );
        Edge collectionToItemEdge = new SimpleEdge(
                cpHeadEntity.getId(),
                CpNamingUtils.getEdgeTypeFromCollectionName( collName ),
                memberEntity.getId(), UUIDUtils.getUUIDLong( memberEntity.getId().getUuid() ) );
        gm.deleteEdge( collectionToItemEdge ).toBlockingObservable().last();

        // remove edge from item to collection
        Edge itemToCollectionEdge = new SimpleEdge(
                memberEntity.getId(),
                CpNamingUtils.getEdgeTypeFromCollectionName(
                        Schema.defaultCollectionName( cpHeadEntity.getId().getType() ) ),
                cpHeadEntity.getId(),
                UUIDUtils.getUUIDLong( cpHeadEntity.getId().getUuid() ) );

        gm.deleteEdge( itemToCollectionEdge ).toBlockingObservable().last();

        // special handling for roles collection of a group
        if ( headEntity.getType().equals( Group.ENTITY_TYPE ) ) {

            if ( collName.equals( COLLECTION_ROLES ) ) {
                String path = ( String ) ( ( Entity ) itemRef ).getMetadata( "path" );

                if ( path.startsWith( "/roles/" ) ) {

                    Entity itemEntity = em.get( new SimpleEntityRef( memberEntity.getId().getType(),
                            memberEntity.getId().getUuid() ) );

                    RoleRef roleRef = SimpleRoleRef.forRoleEntity( itemEntity );
                    em.deleteRole( roleRef.getApplicationRoleName() );
                }
            }
        }
    }


    @Override
    public void copyRelationships(String srcRelationName, EntityRef dstEntityRef,
            String dstRelationName) throws Exception {

        headEntity = em.validate( headEntity );
        dstEntityRef = em.validate( dstEntityRef );

        CollectionInfo srcCollection =
                getDefaultSchema().getCollection( headEntity.getType(), srcRelationName );

        CollectionInfo dstCollection =
                getDefaultSchema().getCollection( dstEntityRef.getType(), dstRelationName );

        Results results = null;
        do {
            if ( srcCollection != null ) {
                results = em.getCollection( headEntity, srcRelationName, null, 5000, Level.REFS, false );
            }
            else {
                results = em.getConnectedEntities( headEntity, srcRelationName, null, Level.REFS );
            }

            if ( ( results != null ) && ( results.size() > 0 ) ) {
                List<EntityRef> refs = results.getRefs();
                for ( EntityRef ref : refs ) {
                    if ( dstCollection != null ) {
                        em.addToCollection( dstEntityRef, dstRelationName, ref );
                    }
                    else {
                        em.createConnection( dstEntityRef, dstRelationName, ref );
                    }
                }
            }
        }
        while ( ( results != null ) && ( results.hasMoreResults() ) );
    }


    @Override
    public Results searchCollection( String collName, Query query ) throws Exception {

        if ( query == null ) {
            query = new Query();
            query.setCollection( collName );
        }

        headEntity = em.validate( headEntity );

        CollectionInfo collection =
            getDefaultSchema().getCollection( headEntity.getType(), collName );

        if ( collection == null ) {
            throw new RuntimeException( "Cannot find collection-info for '" + collName
                    + "' of " + headEntity.getType() + ":" + headEntity .getUuid() );
        }

        final IndexScope indexScope = new IndexScopeImpl(
            cpHeadEntity.getId(),
            CpNamingUtils.getCollectionScopeNameFromCollectionName( collName ) );

        final EntityIndex ei = managerCache.getEntityIndex( applicationScope );

        final SearchTypes types = SearchTypes.fromTypes( collection.getType() );

        logger.debug( "Searching scope {}:{}",

                indexScope.getOwner().toString(), indexScope.getName() );

        query.setEntityType( collection.getType() );
        query = adjustQuery( query );

        // Because of possible stale entities, which are filtered out by buildResults(),
        // we loop until the we've got enough results to satisfy the query limit.

        int maxQueries = 10; // max re-queries to satisfy query limit

        final int originalLimit = query.getLimit();

        Results results = null;
        int queryCount = 0;

        boolean satisfied = false;



        while ( !satisfied && queryCount++ < maxQueries ) {

            CandidateResults crs = ei.search( indexScope, types, query );

            if ( results == null ) {
                logger.debug( "Calling build results 1" );
                results = buildResults( indexScope, query, crs, collName );
            }
            else {
                logger.debug( "Calling build results 2" );
                Results newResults = buildResults(indexScope,  query, crs, collName );
                results.merge( newResults );
            }

            if ( crs.isEmpty() || !crs.hasCursor() ) { // no results, no cursor, can't get more
                satisfied = true;
            }
            else if ( results.size() == originalLimit ) { // got what we need
                satisfied = true;
            }
            else if ( crs.hasCursor() ) {
                satisfied = false;

                // need to query for more
                // ask for just what we need to satisfy, don't want to exceed limit
                query.setCursor( results.getCursor() );
                query.setLimit( originalLimit - results.size() );

                logger.warn( "Satisfy query limit {}, new limit {} query count {}", new Object[] {
                        originalLimit, query.getLimit(), queryCount
                } );
            }
        }

        return results;
    }


    @Override
    @Metered( group = "core", name = "RelationManager_createConnection_connection_ref" )
    public ConnectionRef createConnection( ConnectionRef connection ) throws Exception {

        return createConnection( connection.getConnectionType(), connection.getConnectedEntity() );
    }


    @Override
    @Metered( group = "core", name = "RelationManager_createConnection_connectionType" )
    public ConnectionRef createConnection( String connectionType, EntityRef connectedEntityRef ) throws Exception {

        headEntity = em.validate( headEntity );
        connectedEntityRef = em.validate( connectedEntityRef );

        ConnectionRefImpl connection = new ConnectionRefImpl( headEntity, connectionType, connectedEntityRef );

        CollectionScope targetScope = getCollectionScopeNameFromEntityType(
                applicationScope.getApplication(), connectedEntityRef.getType());

        if ( logger.isDebugEnabled() ) {
            logger.debug("createConnection(): "
                + "Indexing connection type '{}'\n   from source {}:{}]\n"
                + "   to target {}:{}\n   from scope\n   app {}\n   owner {}\n   name {}",
                new Object[] {
                    connectionType,
                    headEntity.getType(),
                    headEntity.getUuid(),
                    connectedEntityRef.getType(),
                    connectedEntityRef.getUuid(),
                    targetScope.getApplication(),
                    targetScope.getOwner(),
                    targetScope.getName()
            });
        }

        Id entityId = new SimpleId( connectedEntityRef.getUuid(), connectedEntityRef.getType());
        org.apache.usergrid.persistence.model.entity.Entity targetEntity =
            ((CpEntityManager)em).load( new CpEntityManager.EntityScope( targetScope, entityId));

        String edgeType = CpNamingUtils.getEdgeTypeFromConnectionType( connectionType );

        // create graph edge connection from head entity to member entity
        Edge edge = new SimpleEdge(
                cpHeadEntity.getId(), edgeType, targetEntity.getId(), System.currentTimeMillis() );

        GraphManager gm = managerCache.getGraphManager( applicationScope );
        gm.writeEdge( edge ).toBlockingObservable().last();

        EntityIndex ei = managerCache.getEntityIndex( applicationScope );
        EntityIndexBatch batch = ei.createBatch();

        // Index the new connection in app|source|type context
        IndexScope indexScope = new IndexScopeImpl( cpHeadEntity.getId(),
                CpNamingUtils.getConnectionScopeName( connectionType ) );

        batch.index( indexScope, targetEntity );

        batch.execute();


        return connection;
    }



    @Override
    @Metered( group = "core", name = "RelationManager_createConnection_paired_connection_type" )
    public ConnectionRef createConnection(
            String pairedConnectionType,
            EntityRef pairedEntity,
            String connectionType,
            EntityRef connectedEntityRef ) throws Exception {

        throw new UnsupportedOperationException( "Paired connections not supported" );
    }


    @Override
    @Metered( group = "core", name = "RelationManager_createConnection_connected_entity_ref" )
    public ConnectionRef createConnection( ConnectedEntityRef... connections ) throws Exception {

        throw new UnsupportedOperationException( "Paired connections not supported" );
    }



    @Override
    public ConnectionRef connectionRef(
            String pairedConnectionType,
            EntityRef pairedEntity,
            String connectionType,
            EntityRef connectedEntityRef ) throws Exception {

        throw new UnsupportedOperationException( "Paired connections not supported" );
    }


    @Override
    public ConnectionRef connectionRef( ConnectedEntityRef... connections ) {

        throw new UnsupportedOperationException( "Paired connections not supported" );
    }


    @Override
    public void deleteConnection( ConnectionRef connectionRef ) throws Exception {

        // First, clean up the dictionary records of the connection
        Keyspace ko = cass.getApplicationKeyspace( applicationId );


        EntityRef connectingEntityRef = connectionRef.getConnectingEntity();  // source
        EntityRef connectedEntityRef = connectionRef.getConnectedEntity();  // target

        String connectionType = connectionRef.getConnectedEntity().getConnectionType();

        CollectionScope targetScope = getCollectionScopeNameFromEntityType( applicationScope.getApplication(),
                connectedEntityRef.getType() );

        if ( logger.isDebugEnabled() ) {
            logger.debug( "Deleting connection '{}' from source {}:{} \n   to target {}:{}",
                new Object[] {
                    connectionType,
                    connectingEntityRef.getType(),
                    connectingEntityRef.getUuid(),
                    connectedEntityRef.getType(),
                    connectedEntityRef.getUuid()
                });
        }

        Id entityId = new SimpleId( connectedEntityRef.getUuid(), connectedEntityRef.getType() );
        org.apache.usergrid.persistence.model.entity.Entity targetEntity =
            ((CpEntityManager)em).load( new CpEntityManager.EntityScope( targetScope, entityId));

        // Delete graph edge connection from head entity to member entity
        Edge edge = new SimpleEdge(
            new SimpleId( connectingEntityRef.getUuid(),
                connectingEntityRef.getType() ),
                connectionType,
                targetEntity.getId(),
                System.currentTimeMillis() );

        GraphManager gm = managerCache.getGraphManager( applicationScope );
        gm.deleteEdge( edge ).toBlockingObservable().last();

        final EntityIndex ei = managerCache.getEntityIndex( applicationScope );
        final EntityIndexBatch batch = ei.createBatch();

        // Deindex the connection in app|source|type context
        IndexScope indexScope = new IndexScopeImpl(
            new SimpleId( connectingEntityRef.getUuid(),
                connectingEntityRef.getType() ),
                CpNamingUtils.getConnectionScopeName( connectionType ) );
        batch.deindex( indexScope, targetEntity );

        // Deindex the connection in app|source|type context
        //TODO REMOVE INDEX CODE
//        IndexScope allTypesIndexScope = new IndexScopeImpl(
//            new SimpleId( connectingEntityRef.getUuid(),
//                connectingEntityRef.getType() ),
//                CpNamingUtils.ALL_TYPES, entityType );
//
//        batch.deindex( allTypesIndexScope, targetEntity );

        batch.execute();
    }



    @Override
    public Set<String> getConnectionTypes() throws Exception {
        return getConnectionTypes( false );
    }


    @Override
    public Set<String> getConnectionTypes( boolean filterConnection ) throws Exception {
        Set<String> connections = cast(
                em.getDictionaryAsSet( headEntity, Schema.DICTIONARY_CONNECTED_TYPES ) );

        if ( connections == null ) {
            return null;
        }
        if ( filterConnection && ( connections.size() > 0 ) ) {
            connections.remove( "connection" );
        }
        return connections;
    }


    @Override
    public Results getConnectedEntities(
            String connectionType, String connectedEntityType, Level level ) throws Exception {

        Results raw = null;

        Preconditions.checkNotNull( connectionType, "connectionType cannot be null" );

        Query query = new Query();
        query.setConnectionType( connectionType );
        query.setEntityType( connectedEntityType );

        if ( connectionType == null ) {
            raw = searchConnectedEntities( query );
        }
        else {

            headEntity = em.validate( headEntity );


            IndexScope indexScope = new IndexScopeImpl(
                    cpHeadEntity.getId(), CpNamingUtils.getConnectionScopeName( connectionType ) );

            final SearchTypes searchTypes = SearchTypes.fromNullableTypes( connectedEntityType );


            final EntityIndex ei = managerCache.getEntityIndex( applicationScope );


            logger.debug("Searching connected entities from scope {}:{}",
                indexScope.getOwner().toString(),
                indexScope.getName());

            query = adjustQuery( query );
            CandidateResults crs = ei.search( indexScope, searchTypes,  query );

            raw = buildResults( indexScope, query, crs, query.getConnectionType() );
        }

        if ( Level.ALL_PROPERTIES.equals( level ) ) {
            List<Entity> entities = new ArrayList<Entity>();
            for ( EntityRef ref : raw.getEntities() ) {
                Entity entity = em.get( ref );
                entities.add( entity );
            }
            return Results.fromEntities( entities );
        }

        List<ConnectionRef> crefs = new ArrayList<ConnectionRef>();
        for ( Entity e : raw.getEntities() ) {
            ConnectionRef cref = new ConnectionRefImpl( headEntity, connectionType, e );
            crefs.add( cref );
        }

        return Results.fromConnections( crefs );
    }


    @Override
    public Results getConnectingEntities(
            String connType, String fromEntityType, Level resultsLevel ) throws Exception {

        return getConnectingEntities( connType, fromEntityType, resultsLevel, -1 );
    }


    @Override
    public Results getConnectingEntities(
            String connType, String fromEntityType, Level level, int count ) throws Exception {

        // looking for edges to the head entity
        String edgeType = CpNamingUtils.getEdgeTypeFromConnectionType( connType );

        Map<EntityRef, Set<String>> containers = getContainers( count, edgeType, fromEntityType );

        if ( Level.REFS.equals( level ) ) {
            List<EntityRef> refList = new ArrayList<EntityRef>( containers.keySet() );
            return Results.fromRefList( refList );
        }

        if ( Level.IDS.equals( level ) ) {
            // TODO: someday this should return a list of Core Persistence Ids
            List<UUID> idList = new ArrayList<UUID>();
            for ( EntityRef ref : containers.keySet() ) {
                idList.add( ref.getUuid() );
            }
            return Results.fromIdList( idList );
        }

        List<Entity> entities = new ArrayList<Entity>();
        for ( EntityRef ref : containers.keySet() ) {
            Entity entity = em.get( ref );
            logger.debug( "   Found connecting entity: " + entity.getProperties() );
            entities.add( entity );
        }
        return Results.fromEntities( entities );
    }


    @Override
    public Results searchConnectedEntities( Query query ) throws Exception {

        Preconditions.checkNotNull(query, "query cannot be null");

        final String connection = query.getConnectionType();

        Preconditions.checkNotNull( connection, "connection must be specified" );

//        if ( query == null ) {
//            query = new Query();
//        }

        headEntity = em.validate( headEntity );

        final IndexScope indexScope = new IndexScopeImpl( cpHeadEntity.getId(),
                CpNamingUtils.getConnectionScopeName( connection ) );

        final SearchTypes searchTypes = SearchTypes.fromNullableTypes( query.getEntityType() );

        EntityIndex ei = managerCache.getEntityIndex( applicationScope );

        logger.debug( "Searching connections from the scope {}:{} with types {}", new Object[] {
                        indexScope.getOwner().toString(), indexScope.getName(), searchTypes
                } );

        query = adjustQuery( query );
        CandidateResults crs = ei.search( indexScope, searchTypes, query );

        return buildConnectionResults( indexScope, query, crs, connection );
    }


    private Query adjustQuery( Query query ) {

        // handle the select by identifier case
        if ( query.getRootOperand() == null ) {

            // a name alias or email alias was specified
            if ( query.containsSingleNameOrEmailIdentifier() ) {

                Identifier ident = query.getSingleIdentifier();

                // an email was specified.  An edge case that only applies to users.
                // This is fulgy to put here, but required.
                if ( query.getEntityType().equals( User.ENTITY_TYPE ) && ident.isEmail() ) {

                    Query newQuery = Query.fromQL( "select * where email='"
                            + query.getSingleNameOrEmailIdentifier() + "'" );
                    query.setRootOperand( newQuery.getRootOperand() );
                }

                // use the ident with the default alias. could be an email
                else {

                    Query newQuery = Query.fromQL( "select * where name='"
                            + query.getSingleNameOrEmailIdentifier() + "'" );
                    query.setRootOperand( newQuery.getRootOperand() );
                }
            }
            else if ( query.containsSingleUuidIdentifier() ) {

                Query newQuery = Query.fromQL(
                        "select * where uuid='" + query.getSingleUuidIdentifier() + "'" );
                query.setRootOperand( newQuery.getRootOperand() );
            }
        }

        if ( query.isReversed() ) {

            Query.SortPredicate desc =
                new Query.SortPredicate( PROPERTY_CREATED, Query.SortDirection.DESCENDING );

            try {
                query.addSort( desc );
            }
            catch ( Exception e ) {
                logger.warn( "Attempted to reverse sort order already set", PROPERTY_CREATED );
            }
        }

        if ( query.getSortPredicates().isEmpty() ) {

            Query.SortPredicate asc =
                new Query.SortPredicate( PROPERTY_CREATED, Query.SortDirection.ASCENDING);

            query.addSort( asc );
        }

        return query;
    }


    @Override
    public Set<String> getConnectionIndexes( String connectionType ) throws Exception {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    private CpRelationManager getRelationManager( EntityRef headEntity ) {
        CpRelationManager rmi = new CpRelationManager();
        rmi.init( em, emf, applicationId, headEntity, null );
        return rmi;
    }


    /** side effect: converts headEntity into an Entity if it is an EntityRef! */
    private Entity getHeadEntity() throws Exception {
        Entity entity = null;
        if ( headEntity instanceof Entity ) {
            entity = ( Entity ) headEntity;
        }
        else {
            entity = em.get( headEntity );
            headEntity = entity;
        }
        return entity;
    }


    private Results buildConnectionResults( final IndexScope indexScope,
            final Query query, final CandidateResults crs, final String connectionType ) {

        if ( query.getLevel().equals( Level.ALL_PROPERTIES ) ) {
            return buildResults( indexScope, query, crs, connectionType );
        }

        final EntityRef sourceRef = new SimpleEntityRef( headEntity.getType(), headEntity.getUuid() );

        List<ConnectionRef> refs = new ArrayList<ConnectionRef>( crs.size() );

        for ( CandidateResult cr : crs ) {

            SimpleEntityRef targetRef =
                    new SimpleEntityRef( cr.getId().getType(), cr.getId().getUuid() );

            final ConnectionRef ref =
                    new ConnectionRefImpl( sourceRef, connectionType, targetRef );

            refs.add( ref );
        }

        return Results.fromConnections( refs );
    }


    /**
     * Build results from a set of candidates, and discard those that represent stale indexes.
     *
     * @param query Query that was executed
     * @param crs Candidates to be considered for results
     * @param collName Name of collection or null if querying all types
     */
    private Results buildResults( final IndexScope indexScope, final Query query,
            final CandidateResults crs, final String collName ) {


        throw new UnsupportedOperationException( "Use a reducer to create results" );
//        logger.debug( "buildResults() for {} from {} candidates", collName, crs.size() );
//
//        //get an instance of our results loader
//        final ResultsLoader resultsLoader = this.resultsLoaderFactory.getLoader(
//                applicationScope, indexScope, query.getResultsLevel() );
//
//        //load the results
//        final Results results = resultsLoader.loadResults( crs );
//
//        //signal for post processing
//        resultsLoader.postProcess();
//
//
//        results.setCursor( crs.getCursor() );
//        results.setQueryProcessor( new CpQueryProcessor( em, query, headEntity, collName ) );
//
//        logger.debug( "Returning results size {}", results.size() );
//
//        return results;
    }




}
