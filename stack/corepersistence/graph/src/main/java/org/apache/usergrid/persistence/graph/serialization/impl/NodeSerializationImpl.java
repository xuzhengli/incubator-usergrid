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

package org.apache.usergrid.persistence.graph.serialization.impl;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.usergrid.persistence.core.javadriver.BatchStatementUtils;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.core.util.ValidationUtils;
import org.apache.usergrid.persistence.graph.Edge;
import org.apache.usergrid.persistence.graph.serialization.NodeSerialization;
import org.apache.usergrid.persistence.graph.serialization.util.GraphValidation;
import org.apache.usergrid.persistence.model.entity.Id;
import org.apache.usergrid.persistence.model.entity.SimpleId;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.inject.Singleton;


/**
 *
 *
 */
@Singleton
public class NodeSerializationImpl implements NodeSerialization {




    /**
     * CFs where the row key contains the source node id
     */
    private static final String GRAPH_MARKED_EDGES = "Graph_Marked_Nodes";

    private static final String GRAPH_MARKED_EDGES_CREATE = "CREATE TABLE IF NOT EXISTS " + GRAPH_MARKED_EDGES
            + " ( scopeId uuid, scopeType varchar, nodeId uuid, nodeType varchar, timestamp bigint,  PRIMARY KEY ((scopeId , scopeType, nodeId, nodeType)) ) WITH caching = 'all';";


    protected final Session session;
    private PreparedStatement insert;
    private PreparedStatement delete;
    private PreparedStatement select;


    @Inject
    public NodeSerializationImpl( final Session session ) {
        this.session = session;
    }


    @PostConstruct
    public void setup() {

    }


    @Override
    public Collection<String> getColumnFamilies() {
        return Collections.singleton( GRAPH_MARKED_EDGES_CREATE );
    }


    @Override
    public void prepareStatements() {
        this.insert = this.session.prepare( "INSERT INTO " + GRAPH_MARKED_EDGES
                + " (scopeId, scopeType , nodeId, nodeType, timestamp) VALUES (?, ?, ?, ?, " +
                "? ) USING TIMESTAMP ?" );

        this.delete = this.session.prepare( "DELETE FROM " + GRAPH_MARKED_EDGES
                + " USING TIMESTAMP ? WHERE scopeId = ? AND scopeType = ? AND nodeId = ? AND nodeType = ? " );


        this.select = this.session.prepare( "SELECT timestamp FROM " + GRAPH_MARKED_EDGES
                + " WHERE scopeId = ? AND scopeType = ? AND nodeId = ? AND nodeType = ? " );
    }


    @Override
    public Collection<? extends Statement> mark( final ApplicationScope scope, final Id node, final long timestamp ) {
        ValidationUtils.validateApplicationScope( scope );
        ValidationUtils.verifyIdentity( node );
        GraphValidation.validateTimestamp( timestamp, "timestamp" );

        final Id applicationId = scope.getApplication();
        final UUID applicationUuid = applicationId.getUuid();
        final String applicationType = applicationId.getType();

        final UUID nodeUuid = node.getUuid();
        final String nodeType = node.getType();

        final BoundStatement statement =
                this.insert.bind( applicationUuid, applicationType, nodeUuid, nodeType, timestamp, timestamp );


        return Collections.singleton( statement );
    }


    @Override
    public Collection<? extends Statement> delete( final ApplicationScope scope, final Id node, final long timestamp ) {
        ValidationUtils.validateApplicationScope( scope );
        ValidationUtils.verifyIdentity( node );
        GraphValidation.validateTimestamp( timestamp, "timestamp" );

        final Id applicationId = scope.getApplication();
        final UUID applicationUuid = applicationId.getUuid();
        final String applicationType = applicationId.getType();

        final UUID nodeUuid = node.getUuid();
        final String nodeType = node.getType();


        final BoundStatement statement =
                this.delete.bind(timestamp, applicationUuid, applicationType, nodeUuid, nodeType );


        return Collections.singleton( statement );
    }


    @Override
    public Optional<Long> getMaxVersion( final ApplicationScope scope, final Id node ) {
        ValidationUtils.validateApplicationScope( scope );
        ValidationUtils.verifyIdentity( node );

        final Id applicationId = scope.getApplication();
        final UUID applicationUuid = applicationId.getUuid();
        final String applicationType = applicationId.getType();

        final UUID nodeUuid = node.getUuid();
        final String nodeType = node.getType();


        final BoundStatement statement =
                this.select.bind( applicationUuid, applicationType, nodeUuid, nodeType );


        final Row row = session.execute( statement ).one();

        if ( row == null  ) {
            return Optional.absent();
        }

        final Long timestamp = row.getLong( "timestamp" );

        return Optional.of( timestamp );
    }


    @Override
    public Map<Id, Long> getMaxVersions( final ApplicationScope scope, final Collection<? extends Edge> edges ) {
        ValidationUtils.validateApplicationScope( scope );
        Preconditions.checkNotNull( edges, "edges cannot be null" );

        final Id applicationId = scope.getApplication();
        final UUID applicationScopeId = applicationId.getUuid();
        final String applicationScopeType = applicationId.getType();

        final Set<Id> uniqueIds = new HashSet<>( edges.size() );

        //worst case all are marked
        final Map<Id, Long> versions = new HashMap<>( edges.size() );

        for ( final Edge edge : edges ) {
            uniqueIds.add( edge.getSourceNode() );
            uniqueIds.add( edge.getTargetNode() );
        }
//
//        List<Statement> statements = new ArrayList<>(uniqueIds.size());
//
//        for(Id uniqueId : uniqueIds){
//            final BoundStatement bound = select.bind( applicationScopeId, applicationScopeType, uniqueId.getUuid(), uniqueId.getType() );
//
//            statements.add( bound );
//        }
//
//        final Prep
//

        final StringBuilder nodeIdInClause = new StringBuilder(  );
        final StringBuilder nodeTypeInClause = new StringBuilder(   );

        for ( Id uniqueId : uniqueIds ) {


            nodeIdInClause.append( uniqueId.getUuid() ).append( " ," );
            nodeTypeInClause.append("'").append( uniqueId.getType() ) .append( "' ," );
        }

        nodeIdInClause.deleteCharAt( nodeIdInClause.length() - 1);
        nodeTypeInClause.deleteCharAt( nodeTypeInClause.length() - 1);

        final StringBuilder builder = new StringBuilder( uniqueIds.size() * 50 );

               builder.append( "select timestamp from " ).append( GRAPH_MARKED_EDGES )
                      .append( " WHERE scopeId = ").append(applicationScopeId).append(" AND  scopeType = '").append(applicationScopeType  ).append("' AND nodeId IN ( " ).append(nodeIdInClause).append(" ) AND nodeType IN ( ").append(nodeTypeInClause ).append( " )" );





        final List<Row> results = session.execute(builder.toString()).all();


        for ( Row row : results ) {

            final long timestamp = row.getLong( "timestamp" );
            final UUID nodeUuid = row.getUUID( "nodeId" );
            final String nodeType = row.getString( "nodeType" );


            final Id nodeId = new SimpleId( nodeUuid, nodeType );


            versions.put( nodeId, timestamp );
        }


        return versions;
    }
}
