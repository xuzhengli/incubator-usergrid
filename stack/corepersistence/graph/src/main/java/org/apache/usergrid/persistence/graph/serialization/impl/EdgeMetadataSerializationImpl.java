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


import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;

import org.apache.usergrid.persistence.core.javadriver.RowIterator;
import org.apache.usergrid.persistence.core.javadriver.RowParser;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.core.util.ValidationUtils;
import org.apache.usergrid.persistence.graph.Edge;
import org.apache.usergrid.persistence.graph.GraphFig;
import org.apache.usergrid.persistence.graph.SearchEdgeType;
import org.apache.usergrid.persistence.graph.SearchIdType;
import org.apache.usergrid.persistence.graph.serialization.EdgeMetadataSerialization;
import org.apache.usergrid.persistence.graph.serialization.util.GraphValidation;
import org.apache.usergrid.persistence.model.entity.Id;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;


/**
 * Class to perform all edge metadata I/O
 */
@Singleton
public class EdgeMetadataSerializationImpl implements EdgeMetadataSerialization {

    /**
     * CFs where the row key contains the source node id
     */
    private static final String CF_SOURCE_EDGE_TYPES = "Graph_Source_Edge_Types";

    private static final String CF_SOURCE_EDGE_TYPES_CREATE = "CREATE TABLE IF NOT EXISTS " + CF_SOURCE_EDGE_TYPES
            + "( scopeId uuid, scopeType varchar, sourceNodeId uuid, sourceNodeType varchar, " +
            "edgeType varchar, PRIMARY KEY ((scopeId, scopeType, sourceNodeId, sourceNodeType), edgeType) )";

    //all target id types for source edge type
    private static final String CF_SOURCE_EDGE_ID_TYPES = "Graph_Source_Edge_Id_Types";


    private static final String CF_SOURCE_EDGE_ID_TYPES_CREATE = "CREATE TABLE IF NOT EXISTS " + CF_SOURCE_EDGE_ID_TYPES
            + "( scopeId uuid, scopeType varchar, sourceNodeId uuid, sourceNodeType varchar, edgeType varchar, " +
            "targetNodeType varchar,  PRIMARY KEY ((scopeId, scopeType, sourceNodeId, sourceNodeType, " +
            "edgeType), targetNodeType) )";


    /**
     * CFs where the row key is the target node id
     */
    private static final String CF_TARGET_EDGE_TYPES = "Graph_Target_Edge_Types";

    private static final String CF_TARGET_EDGE_TYPES_CREATE = "CREATE TABLE IF NOT EXISTS " + CF_TARGET_EDGE_TYPES
            + "( scopeId uuid, scopeType varchar, targetNodeId uuid, targetNodeType varchar, edgeType varchar, " +
            "PRIMARY KEY ((scopeId, scopeType, "
            +
            "targetNodeId, targetNodeType), edgeType ))";


    //all source id types for target edge type
    private static final String CF_TARGET_EDGE_ID_TYPES = "Graph_Target_Edge_Id_Types";

    private static final String CF_TARGET_EDGE_ID_TYPES_CREATE = "CREATE TABLE IF NOT EXISTS " + CF_TARGET_EDGE_ID_TYPES
            + " ( scopeId uuid, scopeType varchar, targetNodeId uuid, targetNodeType varchar, edgeType varchar, "
            + "sourceNodeType varchar, PRIMARY KEY ((scopeId, scopeType, targetNodeId, targetNodeType, " +
            "edgeType), sourceNodeType ) )";


    protected final Session session;
    private final GraphFig graphFig;

    private PreparedStatement sourceEdgeTypesWrite;
    private PreparedStatement sourceEdgeTypesDelete;
    private PreparedStatement sourceEdgeTypesSelect;


    private PreparedStatement targetEdgeTypesWrite;
    private PreparedStatement targetEdgeTypesDelete;
    private PreparedStatement targetEdgeTypesSelect;



    private PreparedStatement sourceEdgeTargetTypesWrite;
    private PreparedStatement sourceEdgeTargetTypesSelect;
    private PreparedStatement sourceEdgeTargetTypesDelete;


    private PreparedStatement targetEdgeSourceTypesWrite;
    private PreparedStatement targetEdgeSourceTypesDelete;
    private PreparedStatement targetEdgeSourceTypesSelect;


    /**
     * TODO.  The version logic in this doesn't work correctly if the highest edge is deleted. We should remove this,
     * and just use a supplied timestamp. This way the caller can determine what max timestamp to use during a delete
     */

    /*
     *
     * @param session
     * @param cassandraConfig
     * @param graphFig
     */
    @Inject
    public EdgeMetadataSerializationImpl( final Session session, final GraphFig graphFig ) {
        Preconditions.checkNotNull( "consistencyFig is required", graphFig );
        Preconditions.checkNotNull( "session is required", session );

        this.session = session;
        this.graphFig = graphFig;
    }


    @Override
    public Collection<? extends Statement> writeEdge( final ApplicationScope scope, final Edge edge ) {

        ValidationUtils.validateApplicationScope( scope );
        GraphValidation.validateEdge( edge );


        final Id applicationId = scope.getApplication();
        final UUID applicationUuid = applicationId.getUuid();
        final String applicationType = applicationId.getType();


        final Id source = edge.getSourceNode();
        final UUID sourceUuid = source.getUuid();
        final String sourceType = source.getType();


        final Id target = edge.getTargetNode();
        final UUID targetUuid = target.getUuid();
        final String targetType = target.getType();

        final String edgeType = edge.getType();

        final long timestamp = edge.getTimestamp();

        final BoundStatement sourceStatement = this.sourceEdgeTypesWrite
                .bind( applicationUuid, applicationType, sourceUuid, sourceType, edgeType, timestamp );


        final BoundStatement sourceTargetTypeStatement = this.sourceEdgeTargetTypesWrite
                .bind( applicationUuid, applicationType, sourceUuid, sourceType, edgeType, targetType, timestamp );

        final BoundStatement targetStatement = this.targetEdgeTypesWrite
                .bind( applicationUuid, applicationType, targetUuid, targetType, edgeType, timestamp );

        final BoundStatement targetSourceTypeStatement = this.targetEdgeSourceTypesWrite
                .bind( applicationUuid, applicationType, targetUuid, targetType, edgeType, sourceType, timestamp );


        return Arrays.asList( sourceStatement, sourceTargetTypeStatement, targetStatement, targetSourceTypeStatement );
    }


    @Override
    public Collection<? extends Statement> removeEdgeTypeFromSource( final ApplicationScope scope, final Edge edge ) {
        return removeEdgeTypeFromSource( scope, edge.getSourceNode(), edge.getType(), edge.getTimestamp() );
    }


    @Override
    public Collection<? extends Statement> removeEdgeTypeFromSource( final ApplicationScope scope, final Id sourceNode,
                                                                     final String type, final long version ) {

        final Id applicationId = scope.getApplication();
        final UUID applicationUuid = applicationId.getUuid();
        final String applicationType = applicationId.getType();
        final UUID sourceId = sourceNode.getUuid();
        final String sourceType = sourceNode.getType();


        final BoundStatement removeStatement = this.sourceEdgeTypesDelete
                .bind( version, applicationUuid, applicationType, sourceId, sourceType, type );

        return Collections.singleton( removeStatement );
    }


    @Override
    public Collection<? extends Statement> removeIdTypeFromSource( final ApplicationScope scope, final Edge edge ) {
        return removeIdTypeFromSource( scope, edge.getSourceNode(), edge.getType(), edge.getTargetNode().getType(),
                edge.getTimestamp() );
    }


    @Override
    public Collection<? extends Statement> removeIdTypeFromSource( final ApplicationScope scope, final Id sourceNode,
                                                                   final String type, final String idType,
                                                                   final long version ) {
        final Id applicationId = scope.getApplication();
        final UUID applicationUuid = applicationId.getUuid();
        final String applicationType = applicationId.getType();

        final UUID sourceId = sourceNode.getUuid();
        final String sourceType = sourceNode.getType();


        final BoundStatement removeStatement = this.sourceEdgeTargetTypesDelete
                .bind( version, applicationUuid, applicationType, sourceId, sourceType, type, idType );

        return Collections.singleton( removeStatement );
    }


    @Override
    public Collection<? extends Statement> removeEdgeTypeToTarget( final ApplicationScope scope, final Edge edge ) {

        return removeEdgeTypeToTarget( scope, edge.getTargetNode(), edge.getType(), edge.getTimestamp() );
    }


    @Override
    public Collection<? extends Statement> removeEdgeTypeToTarget( final ApplicationScope scope, final Id targetNode,
                                                                   final String type, final long version ) {
        final Id applicationId = scope.getApplication();
        final UUID applicationUuid = applicationId.getUuid();
        final String applicationType = applicationId.getType();
        final UUID targetUuid = targetNode.getUuid();
        final String targetType = targetNode.getType();


        final BoundStatement removeStatement = this.targetEdgeTypesDelete
                .bind( version, applicationUuid, applicationType, targetUuid, type, targetType );

        return Collections.singleton( removeStatement );
    }


    @Override
    public Collection<? extends Statement> removeIdTypeToTarget( final ApplicationScope scope, final Edge edge ) {
        return removeIdTypeToTarget( scope, edge.getTargetNode(), edge.getType(), edge.getSourceNode().getType(),
                edge.getTimestamp() );
    }


    @Override
    public Collection<? extends Statement> removeIdTypeToTarget( final ApplicationScope scope, final Id targetNode,
                                                                 final String type, final String idType,
                                                                 final long version ) {


        final Id applicationId = scope.getApplication();
        final UUID applicationUuid = applicationId.getUuid();
        final String applicationType = applicationId.getType();

        final UUID targetUuid = targetNode.getUuid();
        final String targetType = targetNode.getType();


        final BoundStatement removeStatement = this.targetEdgeSourceTypesDelete
                .bind( version, applicationUuid, applicationType, targetUuid, targetType, type, idType );

        return Collections.singleton( removeStatement );
    }


    @Override
    public Iterator<String> getEdgeTypesFromSource( final ApplicationScope scope, final SearchEdgeType search ) {


        final Id applicationId = scope.getApplication();
        final UUID applicationUuid = applicationId.getUuid();
        final String applicationType = applicationId.getType();
        final Id searchNode = search.getNode();
        final UUID sourceId = searchNode.getUuid();
        final String sourceType = searchNode.getType();
        final String start = getStart( search );
        final int limit = graphFig.getScanPageSize();


        final BoundStatement statement =
                this.sourceEdgeTypesSelect.bind( applicationUuid, applicationType, sourceId, sourceType, start );

        statement.setFetchSize( limit );

        return new RowIterator<>( session, statement, EDGE_PARSER );
    }


    @Override
    public Iterator<String> getIdTypesFromSource( final ApplicationScope scope, final SearchIdType search ) {
        final Id applicationId = scope.getApplication();
        final UUID applicationUuid = applicationId.getUuid();
        final String applicationType = applicationId.getType();
        final Id searchNode = search.getNode();
        final UUID sourceId = searchNode.getUuid();
        final String sourceType = searchNode.getType();

        final String edgeType = search.getEdgeType();
        final String start = getStart( search );

        final int limit = graphFig.getScanPageSize();


        final BoundStatement statement = this.sourceEdgeTargetTypesSelect
                .bind( applicationUuid, applicationType, sourceId, sourceType, edgeType, start );

        statement.setFetchSize( limit );

        return new RowIterator<>( session, statement, TARGET_ID_TYPE_PARSER );
    }


    @Override
    public Iterator<String> getEdgeTypesToTarget( final ApplicationScope scope, final SearchEdgeType search ) {
        final Id applicationId = scope.getApplication();
        final UUID applicationUuid = applicationId.getUuid();
        final String applicationType = applicationId.getType();
        final Id searchNode = search.getNode();
        final UUID targetId = searchNode.getUuid();
        final String targetType = searchNode.getType();

        final String start = getStart( search );
        final int limit = graphFig.getScanPageSize();


        final BoundStatement statement =
                this.targetEdgeTypesSelect.bind( applicationUuid, applicationType, targetId, targetType, start );

        statement.setFetchSize( limit );

        return new RowIterator<>( session, statement, EDGE_PARSER );
    }


    @Override
    public Iterator<String> getIdTypesToTarget( final ApplicationScope scope, final SearchIdType search ) {
        final Id applicationId = scope.getApplication();
        final UUID applicationUuid = applicationId.getUuid();
        final String applicationType = applicationId.getType();
        final Id searchNode = search.getNode();
        final UUID targetId = searchNode.getUuid();
        final String targetType = searchNode.getType();
        final String edgeType = search.getEdgeType();

        final String start = getStart( search );

        final int limit = graphFig.getScanPageSize();


        final BoundStatement statement = this.targetEdgeSourceTypesSelect
                .bind( applicationUuid, applicationType, targetId, targetType, edgeType, start );

        statement.setFetchSize( limit );

        return new RowIterator<>( session, statement, SOURCE_ID_TYPE_PARSER );
    }


    @Override
    public Collection<String> getColumnFamilies() {
        return Arrays.asList( CF_SOURCE_EDGE_TYPES_CREATE, CF_SOURCE_EDGE_ID_TYPES_CREATE, CF_TARGET_EDGE_TYPES_CREATE,
                CF_TARGET_EDGE_ID_TYPES_CREATE );
    }


    @Override
    public void prepareStatements() {
        /**
         * Source with edge types
         */
        this.sourceEdgeTypesWrite = session.prepare( "INSERT INTO " + CF_SOURCE_EDGE_TYPES
                + " (scopeId, scopeType, sourceNodeId, sourceNodeType, edgeType) VALUES (?, ?, ?, ?, " +
                "?) USING TIMESTAMP ?" );

        this.sourceEdgeTypesDelete = session.prepare( "DELETE FROM " + CF_SOURCE_EDGE_TYPES
                + " USING TIMESTAMP ? WHERE scopeId = ? AND scopeType = ? AND sourceNodeId = ? AND sourceNodeType = ? "
                +
                "AND edgeType = ? " );


        this.sourceEdgeTypesSelect = session.prepare( "SELECT edgeType FROM " + CF_SOURCE_EDGE_TYPES
                + " WHERE scopeId = ? AND scopeType = ? AND sourceNodeId = ? AND sourceNodeType = ? AND edgeType > ?" );

        /**
         * Source with target types
         */

        this.sourceEdgeTargetTypesWrite = session.prepare( "INSERT INTO " + CF_SOURCE_EDGE_ID_TYPES
                + " (scopeId, scopeType, sourceNodeId, sourceNodeType, edgeType, targetNodeType) VALUES (?, ?, ?, ?, " +
                "?, ?)  USING TIMESTAMP ?" );

        this.sourceEdgeTargetTypesDelete = session.prepare( "DELETE FROM " + CF_SOURCE_EDGE_ID_TYPES
                + " USING TIMESTAMP ? WHERE scopeId = ? AND scopeType = ? AND sourceNodeId = ? AND sourceNodeType = ?" +
                " AND "
                +
                "edgeType = ? AND targetNodeType = ? " );

        this.sourceEdgeTargetTypesSelect = session.prepare( "SELECT targetNodeType FROM " + CF_SOURCE_EDGE_ID_TYPES
                + " WHERE scopeId = ? AND scopeType = ? AND sourceNodeId = ? AND sourceNodeType = ? AND " +
                "edgeType = ? AND targetNodeType > ?" );


        /**
         * Target with edge types
         */

        this.targetEdgeTypesWrite = session.prepare( "INSERT INTO " + CF_TARGET_EDGE_TYPES
                + " (scopeId, scopeType, targetNodeId, targetNodeType, edgeType) VALUES (?, ?, ?, ?, " +
                "?)  USING TIMESTAMP ?" );


        this.targetEdgeTypesDelete = session.prepare( "DELETE FROM " + CF_TARGET_EDGE_TYPES
                + " USING TIMESTAMP ? WHERE scopeId = ? AND scopeType = ? AND targetNodeId = ?  AND edgeType = ? AND targetNodeType = ?" );

        this.targetEdgeTypesSelect = session.prepare( "SELECT edgeType FROM " + CF_TARGET_EDGE_TYPES
                + " WHERE scopeId = ? AND scopeType = ? AND targetNodeId = ? AND targetNodeType = ? AND edgeType > ?" );


        /**
         * Target with source edge types
         */
        this.targetEdgeSourceTypesWrite = session.prepare( "INSERT INTO " + CF_TARGET_EDGE_ID_TYPES
                + " (scopeId, scopeType, targetNodeId, targetNodeType, edgeType, sourceNodeType) VALUES (?, ?, ?, ?, " +
                "?, ?)  USING TIMESTAMP ?" );


        this.targetEdgeSourceTypesDelete = session.prepare( "DELETE FROM " + CF_TARGET_EDGE_ID_TYPES
                + " USING TIMESTAMP ? WHERE scopeId = ? AND scopeType = ? AND targetNodeId = ?  AND targetNodeType = ? AND edgeType = ?" +
                " AND  sourceNodeType = ? " );


        this.targetEdgeSourceTypesSelect = session.prepare( "SELECT sourceNodeType FROM " + CF_TARGET_EDGE_ID_TYPES
                + " WHERE scopeId = ? AND scopeType = ? AND targetNodeId = ? AND targetNodeType = ? AND edgeType = ?" +
                " AND sourceNodeType > ?" );
    }


    private String getStart( final SearchEdgeType search ) {
        final Optional<String> last = search.getLast();

        if ( last.isPresent() ) {
            return last.get();
        }


        final Optional<String> prefix = search.prefix();

        if ( prefix.isPresent() ) {
            return prefix + "\uffff";
        }

        return "";
    }


    private final RowParser<String> EDGE_PARSER = new RowParser<String>() {
        @Override
        public String parseRow( final Row row ) {
            return row.getString( "edgeType" );
        }
    };


    private final RowParser<String> SOURCE_ID_TYPE_PARSER = new RowParser<String>() {
        @Override
        public String parseRow( final Row row ) {
            return row.getString( "sourceNodeType" );
        }
    };

    private final RowParser<String> TARGET_ID_TYPE_PARSER = new RowParser<String>() {
        @Override
        public String parseRow( final Row row ) {
            return row.getString( "targetNodeType" );
        }
    };


    //    private void setStart( final SearchEdgeType search, final RangeBuilder builder ) {
    //        //prefix is set, set our end marker
    //        if ( search.getLast().isPresent() ) {
    //            builder.setEnd( search.getLast().get() );
    //        }
    //
    //        else if ( search.prefix().isPresent() ) {
    //            builder.setStart( search.prefix().get() );
    //        }
    //    }
    //
    //
    //    private void setEnd( final SearchEdgeType search, final RangeBuilder builder ) {
    //        //if our last is set, it takes precendence
    //
    //        if ( search.prefix().isPresent() ) {
    //            builder.setEnd( search.prefix().get() + "\uffff" );
    //        }
    //    }


    /**
     * Simple key object for I/O
     */
    private static class EdgeIdTypeKey {
        private final Id node;
        private final String edgeType;


        private EdgeIdTypeKey( final Id node, final String edgeType ) {
            this.node = node;
            this.edgeType = edgeType;
        }
    }
}
