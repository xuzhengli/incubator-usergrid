/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package org.apache.usergrid.persistence.core.cassandra;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.service.CassandraDaemon;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.io.Resources;


/**
 * Use EmbeddedCassandraFactory
 *
 */
public class EmbeddedCassandra {
    private static final Logger LOG = LoggerFactory.getLogger( EmbeddedCassandra.class );

    public static final int DEFAULT_PORT = 9160;
    public static final int DEFAULT_STORAGE_PORT = 7000;


    private final CassandraDaemon cassandra;


    private static File createTempDir() {
        File tempDir = Files.createTempDir();
        tempDir.deleteOnExit();
        return tempDir;
    }


    public EmbeddedCassandra( File dataDir, String clusterName, int thriftPort, int nativePort, int storagePort )
            throws IOException {
        LOG.info( "Starting cassandra in dir " + dataDir );
        dataDir.mkdirs();

        InputStream is = null;

        try {
            URL templateUrl = EmbeddedCassandra.class.getClassLoader().getResource(
                    "cassandra-template-ug.yaml" );
            Preconditions.checkNotNull( templateUrl, "Cassandra config template is null" );
            String baseFile = Resources.toString( templateUrl, Charset.defaultCharset() );

            String newFile = baseFile.replace( "$DIR$", dataDir.getPath() );
            newFile = newFile.replace( "$THRIFT_PORT$", Integer.toString( thriftPort ) );
            newFile = newFile.replace( "$NATIVE_PORT$", Integer.toString( nativePort ) );
            newFile = newFile.replace( "$STORAGE_PORT$", Integer.toString( storagePort ) );
            newFile = newFile.replace( "$CLUSTER$", clusterName );

            File configFile = new File( dataDir, "cassandra-template.yaml" );
            Files.write( newFile, configFile, Charset.defaultCharset() );

            LOG.info( "Cassandra config file: " + configFile.getPath() );
            System.setProperty( "cassandra.config", "file:" + configFile.getPath() );

            try {
                cassandra = new CassandraDaemon();
                cassandra.init( null );
            }
            catch ( IOException e ) {
                LOG.error( "Error initializing embedded cassandra", e );
                throw e;
            }
        }
        finally {
            Closeables.closeQuietly( is );
        }
        LOG.info( "Started cassandra deamon" );
    }


    public void start() {
        cassandra.start();
    }


    public void stop() {
        cassandra.deactivate();
    }
}
