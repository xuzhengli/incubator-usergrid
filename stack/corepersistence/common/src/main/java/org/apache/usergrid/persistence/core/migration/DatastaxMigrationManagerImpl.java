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

package org.apache.usergrid.persistence.core.migration;


import java.util.Collection;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.usergrid.persistence.core.astyanax.CassandraFig;

import com.datastax.driver.core.Session;
import com.google.inject.Inject;
import com.google.inject.Singleton;


/**
 * Implementation of the migration manager to set up keyspace
 *
 * @author tnine
 */
@Singleton
public class DatastaxMigrationManagerImpl implements DatastaxMigrationManager {


    private static final Logger logger = LoggerFactory.getLogger( DatastaxMigrationManagerImpl.class );

    private final Set<DatastaxMigration> migrations;
    private final Session session;

    private final CassandraFig cassandraFig;


    @Inject
    public DatastaxMigrationManagerImpl( final Session session, final CassandraFig cassandraFig,
                                         final Set<DatastaxMigration> migrations ) {
        this.session = session;
        this.migrations = migrations;
        this.cassandraFig = cassandraFig;
    }


    @Override
    public void migrate() throws MigrationException {


        testAndCreateKeyspace();

        for ( DatastaxMigration migration : migrations ) {

            final Collection<String> columnFamilies = migration.getColumnFamilies();


            if ( columnFamilies == null || columnFamilies.size() == 0 ) {
                logger.warn( "Class {} implements {} but returns null column families for migration.  Either implement"
                                + " this method or remove the interface from the class", migration.getClass(),
                        Migration.class );
                continue;
            }

            for ( String cf : columnFamilies ) {
                testAndCreateColumnFamilyDef( cf );
            }

            /**
             * Now that all the column families have been created intilaize the prepared statements
             */
            migration.prepareStatements();
        }
    }


    /**
     * Check if the column family exists.  If it dosn't create it
     */
    private void testAndCreateColumnFamilyDef( String columnFamilyExecute ) throws MigrationException {

        try {
            session.execute( columnFamilyExecute );
        }
        catch ( Throwable t ) {
            final String message =
                    String.format( "Unable to create column family.  Input string was \"%s\"  ", columnFamilyExecute );
            logger.error( message, t );
            throw new MigrationException( message, t );
        }
    }


    /**
     * Check if they keyspace exists.  If it doesn't create it
     */
    private void testAndCreateKeyspace() {


        final String keyspaceName = cassandraFig.getKeyspaceName();


        final String createStatement = "CREATE KEYSPACE IF NOT EXISTS " + keyspaceName
                + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
        session.execute( createStatement );

        //set our keyspace
        session.execute( "USE " + keyspaceName );
    }
}
