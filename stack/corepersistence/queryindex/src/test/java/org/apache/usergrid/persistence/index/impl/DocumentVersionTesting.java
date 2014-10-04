/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  The ASF licenses this file to You
 * under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.  For additional information regarding
 * copyright in this work, please see the NOTICE file in the top level
 * directory of this distribution.
 */
package org.apache.usergrid.persistence.index.impl;


import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.rest.RestStatus;
import org.jukito.UseModules;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.RandomStringUtils;

import org.apache.usergrid.persistence.core.cassandra.ITRunner;
import org.apache.usergrid.persistence.index.guice.TestIndexModule;
import org.apache.usergrid.persistence.model.util.UUIDGenerator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * A POC that proves some bassic
 */
@RunWith( ITRunner.class )
@UseModules( { TestIndexModule.class } )
public class DocumentVersionTesting extends BaseIT {

    private static final Logger log = LoggerFactory.getLogger( DocumentVersionTesting.class );
    public static final String TYPE = "type";

    private static Client client;


    private String indexName;

    private int counter = 0;


    @Before
    public void setIndexName() {
        indexName = "testindex" + System.currentTimeMillis();
    }


    @BeforeClass
    public static void createClient() {


        String clusterName = "usergrid";

        String allHosts = "localhost:9300";

        String nodeName = null;

        try {
            nodeName = InetAddress.getLocalHost().getHostName();
        }
        catch ( UnknownHostException ex ) {
            nodeName = "client-" + RandomStringUtils.randomAlphabetic( 8 );
            log.warn( "Couldn't get hostname to use as ES node name, using " + nodeName );
        }


        Settings settings = ImmutableSettings.settingsBuilder()

                .put( "cluster.name", clusterName )

                        // this assumes that we're using zen for host discovery.  Putting an
                        // explicit set of bootstrap hosts ensures we connect to a valid cluster.
                .put( "discovery.zen.ping.unicast.hosts", allHosts )
                .put( "discovery.zen.ping.multicast.enabled", "false" ).put( "http.enabled", false )

                .put( "client.transport.ping_timeout", 2000 ) // milliseconds
                .put( "client.transport.nodes_sampler_interval", 100 ).put( "network.tcp.blocking", true )
                .put( "node.name", nodeName )

                .build();

        log.debug( "Creating ElasticSearch client with settings: " + settings.getAsMap() );

        Node node = NodeBuilder.nodeBuilder().settings( settings ).client( true ).node();

        client = node.client();
    }


    /**
     * Tests that when we specify versions, old versions aren't accepted. Intentionally DOES NOT flush,
     * we want to ensure version correctness without a flush
     */
    @Test
    public void testCustomIdempotentVersion() throws IOException, ExecutionException, InterruptedException {

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        final UUID indexId = UUIDGenerator.newTimeUUID();

        final UUID v1 = UUIDGenerator.newTimeUUID();

        final long v1Timestamp = v1.timestamp();

        final UUID v2 = UUIDGenerator.newTimeUUID();

        final long v2Timestamp = v2.timestamp();

        final UUID v3 = UUIDGenerator.newTimeUUID();

        final long v3Timestamp = v3.timestamp();


        final Map<String, Object> fields = new HashMap<>();

        fields.put( "field1", 1 );


        IndexRequestBuilder indexOp = client.prepareIndex( indexName, TYPE, indexId.toString() );

        indexOp.setVersionType( VersionType.EXTERNAL_GTE ).setVersion( v1Timestamp );


        indexOp.setSource( fields );

        bulkRequestBuilder.add( indexOp );

        BulkResponse bulkResponse = bulkRequestBuilder.get();
        assertFalse( bulkResponse.hasFailures() );

        //now write V3

        fields.put( "field1", 3 );
        fields.put( "field2", "new" );


        indexOp = client.prepareIndex( indexName, TYPE, indexId.toString() );

        indexOp.setVersionType( VersionType.EXTERNAL_GTE ).setVersion( v3Timestamp );


        indexOp.setSource( fields );

        bulkRequestBuilder.add( indexOp );

        bulkResponse = bulkRequestBuilder.get();
        assertFalse( bulkResponse.hasFailures() );


        GetResponse response = client.get( client.prepareGet( indexName, TYPE, indexId.toString() ).request() ).get();

        assertEquals( v3.timestamp(), response.getVersion() );

        //now try to update, it shouldn't take

        indexOp = client.prepareIndex( indexName, TYPE, indexId.toString() );

        indexOp.setVersionType( VersionType.EXTERNAL_GTE ).setVersion( v2Timestamp );


        indexOp.setSource( fields );

        bulkRequestBuilder.add( indexOp );
        bulkResponse = bulkRequestBuilder.get();

        assertTrue( bulkResponse.hasFailures() );

        BulkItemResponse.Failure failure = bulkResponse.getItems()[0].getFailure();

        RestStatus status = failure.getStatus();

        assertEquals( RestStatus.CONFLICT, status );
    }


    /**
     * Tests when we update via PUT, we set all our fields for the entire entity except our reserved array
     */
    @Test
    @Ignore("This fails, we can't take this approach")
    public void testPutUpdatesFields() throws IOException, ExecutionException, InterruptedException {

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        final UUID indexId = UUIDGenerator.newTimeUUID();

        final long v1Timestamp = 1;

        final long v2Timestamp = 2;


        final String[] arrayValues = new String[] { "edge1", "edge2", "edge3" };

        final Map<String, Object> fields = new HashMap<>();

        fields.put( "ordinal", 1 );
        fields.put( "edges", arrayValues );


        IndexRequestBuilder indexOp = client.prepareIndex( indexName, TYPE, indexId.toString() );

        indexOp.setVersionType( VersionType.EXTERNAL_GTE ).setVersion( v1Timestamp );


        indexOp.setSource( fields );

        bulkRequestBuilder.add( indexOp );

        BulkResponse bulkResponse = bulkRequestBuilder.get();
        assertFalse( bulkResponse.hasFailures() );

        //now write v2, we update ordinal but don't touch the original fields
        fields.clear();
        fields.put( "ordinal", 2 );

        UpdateRequestBuilder updateOp = client.prepareUpdate( indexName, TYPE, indexId.toString() );

        updateOp.setVersionType( VersionType.EXTERNAL_GTE ).setVersion( v1Timestamp );

        updateOp.setDoc( fields );

        bulkRequestBuilder.add( updateOp );

        //force the refresh so we can read
        bulkResponse = bulkRequestBuilder.setRefresh( true ).get();
        assertFalse( bulkResponse.hasFailures() );


        GetResponse response = client.get( client.prepareGet( indexName, TYPE, indexId.toString() ).request() ).get();

        assertEquals( v2Timestamp, response.getVersion() );

        int returned = ( int ) response.getField( "ordinal" ).getValue();

        assertEquals( 2, returned );


        List<Object> values = response.getField( "edges" ).getValues();

        assertTrue( values.contains( arrayValues[0] ) );
        assertTrue( values.contains( arrayValues[1] ) );
        assertTrue( values.contains( arrayValues[2] ) );
    }
}
