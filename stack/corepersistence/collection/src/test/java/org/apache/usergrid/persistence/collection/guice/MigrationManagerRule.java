package org.apache.usergrid.persistence.collection.guice;


import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.usergrid.persistence.core.migration.DatastaxMigration;
import org.apache.usergrid.persistence.core.migration.DatastaxMigrationManager;
import org.apache.usergrid.persistence.core.migration.MigrationException;
import org.apache.usergrid.persistence.core.migration.MigrationManager;

import com.google.inject.Inject;
import com.google.inject.Singleton;


/**
 */
@Singleton
public class MigrationManagerRule extends ExternalResource {
    private static final Logger LOG = LoggerFactory.getLogger( MigrationManagerRule.class );


    private DatastaxMigrationManager datastaxMigration;


    @Inject
    public void setDsMigrationManager(final DatastaxMigrationManager datastaxMigration){
        this.datastaxMigration = datastaxMigration;
    }


    @Override
    protected void before() throws MigrationException {
        LOG.info( "Starting migration" );

        datastaxMigration.migrate();

        LOG.info( "Migration complete" );
    }
}
