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

package org.apache.usergrid.persistence.graph.serialization;


import java.util.Iterator;
import java.util.UUID;

import org.jukito.UseModules;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.usergrid.persistence.collection.guice.MigrationManagerRule;
import org.apache.usergrid.persistence.core.cassandra.ITRunner;
import org.apache.usergrid.persistence.core.javadriver.BatchStatementUtils;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.graph.Edge;
import org.apache.usergrid.persistence.graph.guice.TestGraphModule;
import org.apache.usergrid.persistence.model.entity.Id;
import org.apache.usergrid.persistence.model.util.UUIDGenerator;

import com.datastax.driver.core.Session;
import com.fasterxml.uuid.UUIDComparator;
import com.google.inject.Inject;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;

import static org.apache.usergrid.persistence.graph.test.util.EdgeTestUtils.createEdge;
import static org.apache.usergrid.persistence.graph.test.util.EdgeTestUtils.createId;
import static org.apache.usergrid.persistence.graph.test.util.EdgeTestUtils.createSearchEdge;
import static org.apache.usergrid.persistence.graph.test.util.EdgeTestUtils.createSearchIdType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 *
 *
 */
@RunWith(ITRunner.class)
@UseModules({ TestGraphModule.class })
public class EdgeMetadataSerializationTest {

    @Inject
    @Rule
    public MigrationManagerRule migrationManagerRule;


    @Inject
    protected EdgeMetadataSerialization serialization;

    @Inject
    protected Session session;


    protected ApplicationScope scope;


    @Before
    public void setup() {
        scope = mock( ApplicationScope.class );

        Id orgId = mock( Id.class );

        when( orgId.getType() ).thenReturn( "organization" );
        when( orgId.getUuid() ).thenReturn( UUIDGenerator.newTimeUUID() );

        when( scope.getApplication() ).thenReturn( orgId );


        //TODO We shouldn't have to do this, but our injector lifecycle it borked in testing  The migration
        //is running with a different instance than the injected one.
        serialization.prepareStatements();
    }


    /**
     * Test write and read edge types from source -> target
     */
    @Test
    public void readTargetEdgeTypes() throws ConnectionException {
        final Edge edge1 = createEdge( "source", "edge", "target" );

        final Id sourceId = edge1.getSourceNode();

        final Edge edge2 = createEdge( sourceId, "edge", createId( "target2" ) );

        final Edge edge3 = createEdge( sourceId, "edge2", createId( "target3" ) );

        //set writing the edge
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge1 ));
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge2 ));
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge3 ));

        //now check we get both types back
        Iterator<String> edges = serialization.getEdgeTypesFromSource( scope, createSearchEdge( sourceId, null ) );

        assertEquals( "edge", edges.next() );
        assertEquals( "edge2", edges.next() );
        assertFalse( edges.hasNext() );

        //now check we can resume correctly with a "last"

        edges = serialization.getEdgeTypesFromSource( scope, createSearchEdge( sourceId, "edge" ) );

        assertEquals( "edge2", edges.next() );
        assertFalse( edges.hasNext() );
    }


    /**
     * Test write and read edge types from source -> target
     */
    @Test
    public void readSourceEdgeTypes() throws ConnectionException {
        final Edge edge1 = createEdge( "source", "edge", "target" );

        final Id targetId = edge1.getTargetNode();

        final Edge edge2 = createEdge( createId( "source" ), "edge", targetId );

        final Edge edge3 = createEdge( createId( "source2" ), "edge2", targetId );

        //set writing the edge
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge1 ));
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge2 ));
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge3 ));

        //now check we get both types back
        Iterator<String> edges = serialization.getEdgeTypesToTarget( scope, createSearchEdge( targetId, null ) );

        assertEquals( "edge", edges.next() );
        assertEquals( "edge2", edges.next() );
        assertFalse( edges.hasNext() );

        //now check we can resume correctly with a "last"

        edges = serialization.getEdgeTypesToTarget( scope, createSearchEdge( targetId, "edge" ) );

        assertEquals( "edge2", edges.next() );
        assertFalse( edges.hasNext() );
    }


    /**
     * Test write and read edge types from source -> target
     */
    @Test
    public void readTargetEdgeIdTypes() throws ConnectionException {
        final Edge edge1 = createEdge( "source", "edge", "target" );

        final Id sourceId = edge1.getSourceNode();

        final Edge edge2 = createEdge( sourceId, "edge", createId( "target" ) );

        final Edge edge3 = createEdge( sourceId, "edge", createId( "target2" ) );

        //shouldn't be returned
        final Edge edge4 = createEdge( sourceId, "edge2", createId( "target3" ) );

        //set writing the edge
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge1 ));
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge2 ));
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge3 ));
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge4 ));

        //now check we get both types back
        Iterator<String> types =
                serialization.getIdTypesFromSource( scope, createSearchIdType( sourceId, "edge", null ) );

        assertEquals( "target", types.next() );
        assertEquals( "target2", types.next() );
        assertFalse( types.hasNext() );


        //now check we can resume correctly with a "last"

        types = serialization.getIdTypesFromSource( scope, createSearchIdType( sourceId, "edge", "target" ) );

        assertEquals( "target2", types.next() );
        assertFalse( types.hasNext() );


        //check by other type
        types = serialization.getIdTypesFromSource( scope, createSearchIdType( sourceId, "edge2", null ) );
        assertEquals( "target3", types.next() );
        assertFalse( types.hasNext() );
    }


    /**
     * Test write and read edge types from source -> target
     */
    @Test
    public void readSourceEdgeIdTypes() throws ConnectionException {
        final Edge edge1 = createEdge( "source", "edge", "target" );

        final Id targetId = edge1.getTargetNode();

        final Edge edge2 = createEdge( createId( "source" ), "edge", targetId );

        final Edge edge3 = createEdge( createId( "source2" ), "edge", targetId );

        //shouldn't be returned
        final Edge edge4 = createEdge( createId( "source3" ), "edge2", targetId );

        //set writing the edge
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge1 ));
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge2 ));
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge3 ));
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge4 ));

        //now check we get both types back
        Iterator<String> types =
                serialization.getIdTypesToTarget( scope, createSearchIdType( targetId, "edge", null ) );

        assertEquals( "source", types.next() );
        assertEquals( "source2", types.next() );
        assertFalse( types.hasNext() );


        //now check we can resume correctly with a "last"

        types = serialization.getIdTypesToTarget( scope, createSearchIdType( targetId, "edge", "source" ) );

        assertEquals( "source2", types.next() );
        assertFalse( types.hasNext() );


        //check by other type
        types = serialization.getIdTypesToTarget( scope, createSearchIdType( targetId, "edge2", null ) );
        assertEquals( "source3", types.next() );
        assertFalse( types.hasNext() );
    }


    /**
     * Test write and read edge types from source -> target
     */
    @Test
    public void deleteTargetEdgeTypes() throws ConnectionException {
        final long timestamp = 1000l;
        final Edge edge1 = createEdge( "source", "edge", "target", timestamp );

        final Id sourceId = edge1.getSourceNode();

        final Edge edge2 = createEdge( sourceId, "edge", createId( "target2" ), timestamp + 1 );

        final Edge edge3 = createEdge( sourceId, "edge2", createId( "target3" ), timestamp + 2 );

        //set writing the edge
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge1 ));
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge2 ));
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge3 ));

        //now check we get both types back
        Iterator<String> edges = serialization.getEdgeTypesFromSource( scope, createSearchEdge( sourceId, null ) );

        assertEquals( "edge", edges.next() );
        assertEquals( "edge2", edges.next() );
        assertFalse( edges.hasNext() );

        //this shouldn't remove the edge, since edge1 has a version < edge2
        BatchStatementUtils.runBatches( session, serialization.removeEdgeTypeFromSource( scope, edge1 ));

        edges = serialization.getEdgeTypesFromSource( scope, createSearchEdge( sourceId, null ) );

        assertEquals( "edge", edges.next() );
        assertEquals( "edge2", edges.next() );
        assertFalse( edges.hasNext() );

        //this should delete it. The version is the max for that edge type
        BatchStatementUtils.runBatches( session, serialization.removeEdgeTypeFromSource( scope, edge2 ));


        //now check we have 1 left
        edges = serialization.getEdgeTypesFromSource( scope, createSearchEdge( sourceId, null ) );

        assertEquals( "edge2", edges.next() );
        assertFalse( edges.hasNext() );

        BatchStatementUtils.runBatches( session, serialization.removeEdgeTypeFromSource( scope, edge3 ));

        //check we have nothing
        edges = serialization.getEdgeTypesFromSource( scope, createSearchEdge( sourceId, null ) );

        assertFalse( edges.hasNext() );
    }


    /**
     * Test write and read edge types from source -> target
     */
    @Test
    public void deleteSourceEdgeTypes() throws ConnectionException {
        final Edge edge1 = createEdge( "source", "edge", "target" );

        final Id targetId = edge1.getTargetNode();

        final Edge edge2 = createEdge( createId( "source" ), "edge", targetId );

        final Edge edge3 = createEdge( createId( "source2" ), "edge2", targetId );

        //set writing the edge
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge1 ));
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge2 ));
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge3 ));


        //now check we get both types back
        Iterator<String> edges = serialization.getEdgeTypesToTarget( scope, createSearchEdge( targetId, null ) );

        assertEquals( "edge", edges.next() );
        assertEquals( "edge2", edges.next() );
        assertFalse( edges.hasNext() );

        //this shouldn't remove the edge, since edge1 has a version < edge2
        BatchStatementUtils.runBatches( session, serialization.removeEdgeTypeFromSource( scope, edge1 ));

        edges = serialization.getEdgeTypesToTarget( scope, createSearchEdge( targetId, null ) );

        assertEquals( "edge", edges.next() );
        assertEquals( "edge2", edges.next() );
        assertFalse( edges.hasNext() );


        BatchStatementUtils.runBatches( session, serialization.removeEdgeTypeToTarget( scope, edge2 ));

        //now check we have 1 left
        edges = serialization.getEdgeTypesToTarget( scope, createSearchEdge( targetId, null ) );

        assertEquals( "edge2", edges.next() );
        assertFalse( edges.hasNext() );

        BatchStatementUtils.runBatches( session, serialization.removeEdgeTypeToTarget( scope, edge3 ));

        //check we have nothing
        edges = serialization.getEdgeTypesToTarget( scope, createSearchEdge( targetId, null ) );

        assertFalse( edges.hasNext() );
    }


    /**
     * Test write and read edge types from source -> target
     */
    @Test
    public void deleteTargetIdTypes() throws ConnectionException {

        final long timestamp = 1000l;

        final Edge edge1 = createEdge( "source", "edge", "target", timestamp );

        final Id sourceId = edge1.getSourceNode();

        final Edge edge2 = createEdge( sourceId, "edge", createId( "target" ), timestamp + 1 );

        final Edge edge3 = createEdge( sourceId, "edge", createId( "target2" ), timestamp + 2 );

        //set writing the edge
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge1 ));
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge2 ));
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge3 ));

        //now check we get both types back
        Iterator<String> edges =
                serialization.getIdTypesFromSource( scope, createSearchIdType( sourceId, "edge", null ) );

        assertEquals( "target", edges.next() );
        assertEquals( "target2", edges.next() );
        assertFalse( edges.hasNext() );

        //this shouldn't remove the edge, since edge1 has a version < edge2
        BatchStatementUtils.runBatches( session, serialization.removeIdTypeFromSource( scope, edge1 ));

        edges = serialization.getIdTypesFromSource( scope, createSearchIdType( sourceId, "edge", null ) );

        assertEquals( "target", edges.next() );
        assertEquals( "target2", edges.next() );
        assertFalse( edges.hasNext() );

        //this should delete it. The version is the max for that edge type
        BatchStatementUtils.runBatches( session, serialization.removeIdTypeFromSource( scope, edge2 ));


        //now check we have 1 left
        edges = serialization.getIdTypesFromSource( scope, createSearchIdType( sourceId, "edge", null ) );

        assertEquals( "target2", edges.next() );
        assertFalse( edges.hasNext() );

        BatchStatementUtils.runBatches( session, serialization.removeIdTypeFromSource( scope, edge3 ));

        //check we have nothing
        edges = serialization.getIdTypesFromSource( scope, createSearchIdType( sourceId, "edge", null ) );

        assertFalse( edges.hasNext() );
    }


    /**
     * Test write and read edge types from source -> target
     */
    @Test
    public void deleteSourceIdTypes() throws ConnectionException {

        final long timestamp = 1000l;

        final Edge edge1 = createEdge( "source", "edge", "target", timestamp );

        final Id targetId = edge1.getTargetNode();

        final Edge edge2 = createEdge( createId( "source" ), "edge", targetId, timestamp+1 );

        final Edge edge3 = createEdge( createId( "source2" ), "edge", targetId, timestamp+2 );

        //set writing the edge
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge1 ));
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge2 ));
        BatchStatementUtils.runBatches( session, serialization.writeEdge( scope, edge3 ));


        //now check we get both types back
        Iterator<String> edges =
                serialization.getIdTypesToTarget( scope, createSearchIdType( targetId, "edge", null ) );

        assertEquals( "source", edges.next() );
        assertEquals( "source2", edges.next() );
        assertFalse( edges.hasNext() );

        //this shouldn't remove the edge, since edge1 has a version < edge2
        BatchStatementUtils.runBatches( session, serialization.removeIdTypeToTarget( scope, edge1 ));

        edges = serialization.getIdTypesToTarget( scope, createSearchIdType( targetId, "edge", null ) );

        assertEquals( "source", edges.next() );
        assertEquals( "source2", edges.next() );
        assertFalse( edges.hasNext() );


        BatchStatementUtils.runBatches( session, serialization.removeIdTypeToTarget( scope, edge2 ));

        //now check we have 1 left
        edges = serialization.getIdTypesToTarget( scope, createSearchIdType( targetId, "edge", null ) );

        assertEquals( "source2", edges.next() );
        assertFalse( edges.hasNext() );

        BatchStatementUtils.runBatches( session, serialization.removeIdTypeToTarget( scope, edge3 ));

        //check we have nothing
        edges = serialization.getIdTypesToTarget( scope, createSearchIdType( targetId, "edge", null ) );

        assertFalse( edges.hasNext() );
    }


//    @Test
//    public void validateDeleteCollision() throws ConnectionException {
//
//
//        final String CF_NAME = "test";
//        final StringSerializer STR_SER = StringSerializer.get();
//
//
//
//        ColumnFamily<String, String> testCf = new ColumnFamily<String, String>( CF_NAME, STR_SER, STR_SER );
//
//        if(keyspace.describeKeyspace().getColumnFamily( CF_NAME ) == null){
//            keyspace.createColumnFamily( testCf, null );
//        }
//
//
//
//
//        final String key = "key";
//        final String colname = "name";
//        final String colvalue = "value";
//
//        UUID firstUUID = UUIDGenerator.newTimeUUID();
//
//        UUID secondUUID = UUIDGenerator.newTimeUUID();
//
//        UUID thirdUUID = UUIDGenerator.newTimeUUID();
//
//        assertTrue( "First before second", UUIDComparator.staticCompare( firstUUID, secondUUID ) < 0 );
//
//        assertTrue( "Second before third", UUIDComparator.staticCompare( secondUUID, thirdUUID ) < 0 );
//
//        MutationBatch batch = keyspace.prepareMutationBatch();
//
//        batch.withRow( testCf, key ).setTimestamp( firstUUID.timestamp() ).putColumn( colname, colvalue );
//
//        batch);
//
//        //now read it back to validate
//
//        Column<String> col = keyspace.prepareQuery( testCf ).getKey( key ).getColumn( colname )).getResult();
//
//        assertEquals( colname, col.getName() );
//        assertEquals( colvalue, col.getStringValue() );
//
//        //now issue a write and a delete with the same timestamp, write will win
//
//        batch = keyspace.prepareMutationBatch();
//        batch.withRow( testCf, key ).setTimestamp( firstUUID.timestamp() ).putColumn( colname, colvalue );
//        batch.withRow( testCf, key ).setTimestamp( firstUUID.timestamp() ).deleteColumn( colname );
//        batch);
//
//        boolean deleted = false;
//
//        try {
//            keyspace.prepareQuery( testCf ).getKey( key ).getColumn( colname )).getResult();
//        }
//        catch ( NotFoundException nfe ) {
//            deleted = true;
//        }
//
//        assertTrue( deleted );
//
//        //ensure that if we have a latent write, it won't overwrite a newer value
//        batch.withRow( testCf, key ).setTimestamp( secondUUID.timestamp() ).putColumn( colname, colvalue );
//        batch);
//
//        col = keyspace.prepareQuery( testCf ).getKey( key ).getColumn( colname )).getResult();
//
//        assertEquals( colname, col.getName() );
//        assertEquals( colvalue, col.getStringValue() );
//
//        //now issue a delete with the first timestamp, column should still be present
//        batch = keyspace.prepareMutationBatch();
//        batch.withRow( testCf, key ).setTimestamp( firstUUID.timestamp() ).deleteColumn( colname );
//        batch);
//
//
//        col = keyspace.prepareQuery( testCf ).getKey( key ).getColumn( colname )).getResult();
//
//        assertEquals( colname, col.getName() );
//        assertEquals( colvalue, col.getStringValue() );
//
//        //now delete it with the 3rd timestamp, it should disappear
//
//        batch = keyspace.prepareMutationBatch();
//        batch.withRow( testCf, key ).setTimestamp( thirdUUID.timestamp() ).deleteColumn( colname );
//        batch);
//
//        deleted = false;
//
//        try {
//            keyspace.prepareQuery( testCf ).getKey( key ).getColumn( colname )).getResult();
//        }
//        catch ( NotFoundException nfe ) {
//            deleted = true;
//        }
//
//        assertTrue( deleted );
//    }
}
