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
package org.apache.usergrid.persistence.collection.mvcc.stage.write;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixThreadPoolProperties;

import org.apache.usergrid.persistence.collection.CollectionMembers;
import org.apache.usergrid.persistence.collection.serialization.impl.CollectionMembersImpl;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.model.util.EntityUtils;
import org.apache.usergrid.persistence.core.astyanax.CassandraConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.usergrid.persistence.collection.CollectionScope;
import org.apache.usergrid.persistence.collection.exception.WriteUniqueVerifyException;
import org.apache.usergrid.persistence.collection.MvccEntity;
import org.apache.usergrid.persistence.collection.mvcc.entity.MvccValidationUtils;
import org.apache.usergrid.persistence.collection.mvcc.stage.CollectionIoEvent;
import org.apache.usergrid.persistence.collection.serialization.SerializationFig;
import org.apache.usergrid.persistence.collection.serialization.UniqueValue;
import org.apache.usergrid.persistence.collection.serialization.UniqueValueSerializationStrategy;
import org.apache.usergrid.persistence.collection.serialization.UniqueValueSet;
import org.apache.usergrid.persistence.collection.serialization.impl.UniqueValueImpl;
import org.apache.usergrid.persistence.model.entity.Entity;
import org.apache.usergrid.persistence.model.entity.Id;
import org.apache.usergrid.persistence.model.field.Field;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import rx.functions.Action1;


/**
 * This phase execute all unique value verification on the MvccEntity.
 */
@Singleton
public class WriteUniqueVerify implements Action1<CollectionIoEvent<MvccEntity>> {

    private static final Logger LOG = LoggerFactory.getLogger( WriteUniqueVerify.class );

    private final UniqueValueSerializationStrategy uniqueValueStrat;

    protected final SerializationFig serializationFig;

    protected final Keyspace keyspace;
    private final CassandraConfig cassandraFig;


    @Inject
    public WriteUniqueVerify( final UniqueValueSerializationStrategy uniqueValueSerializiationStrategy,
                              final SerializationFig serializationFig, final Keyspace keyspace, final CassandraConfig cassandraFig ) {
        this.keyspace = keyspace;
        this.cassandraFig = cassandraFig;

        Preconditions.checkNotNull( uniqueValueSerializiationStrategy, "uniqueValueSerializationStrategy is required" );
        Preconditions.checkNotNull( serializationFig, "serializationFig is required" );

        this.uniqueValueStrat = uniqueValueSerializiationStrategy;
        this.serializationFig = serializationFig;
    }


    @Override
    public void call( final CollectionIoEvent<MvccEntity> ioevent ) {

        MvccValidationUtils.verifyMvccEntityWithEntity( ioevent.getEvent() );

        final MvccEntity mvccEntity = ioevent.getEvent();

        final Entity entity = mvccEntity.getEntity().get();

        final CollectionScope scope = ioevent.getEntityCollection();
        final ApplicationScope applicationScope = ioevent.getApplicationScope();

        final MutationBatch batch = keyspace.prepareMutationBatch();
        //allocate our max size, worst case
        final List<Field> uniqueFields = new ArrayList<>( entity.getFields().size() );

        //
        // Construct all the functions for verifying we're unique
        //


        for ( final Field field : EntityUtils.getUniqueFields(entity)) {

            // if it's unique, create a function to validate it and add it to the list of
            // concurrent validations

            // use write-first then read strategy
            final UniqueValue written = new UniqueValueImpl( field, mvccEntity.getId(), mvccEntity.getVersion() );

            // use TTL in case something goes wrong before entity is finally committed
            final MutationBatch mb = uniqueValueStrat.write( applicationScope, scope, written, serializationFig.getTimeout() );

            batch.mergeShallow( mb );
            uniqueFields.add(field);
        }

        //short circuit nothing to do
        if ( uniqueFields.size() == 0 ) {
            return  ;
        }

        //perform the write
        try {
            batch.execute();
        }
        catch ( ConnectionException ex ) {
            throw new RuntimeException( "Unable to write to cassandra", ex );
        }

        // use simple thread pool to verify fields in parallel
        ConsistentReplayCommand cmd = new ConsistentReplayCommand(uniqueValueStrat,cassandraFig, applicationScope, scope, uniqueFields,entity);
        Map<String,Field>  uniquenessViolations = cmd.execute();
         cmd.getFailedExecutionException();
        //We have violations, throw an exception
        if ( !uniquenessViolations.isEmpty() ) {
            throw new WriteUniqueVerifyException( mvccEntity, applicationScope, ioevent.getEntityCollection(), uniquenessViolations );
        }
    }

    private static class ConsistentReplayCommand extends HystrixCommand<Map<String,Field>>{

        private final UniqueValueSerializationStrategy uniqueValueSerializationStrategy;
        private final CassandraConfig fig;
        private final ApplicationScope applicationScope;
        private final CollectionScope collectionScope;
        private final Collection<Field> uniqueFields;
        private final Entity entity;

        public ConsistentReplayCommand( UniqueValueSerializationStrategy uniqueValueSerializationStrategy,
                                        CassandraConfig fig, final ApplicationScope applicationScope, final CollectionScope collectionScope, Collection<Field> uniqueFields, Entity entity ){
            super(REPLAY_GROUP);
            this.uniqueValueSerializationStrategy = uniqueValueSerializationStrategy;
            this.fig = fig;
            this.applicationScope = applicationScope;
            this.collectionScope = collectionScope;
            this.uniqueFields = uniqueFields;
            this.entity = entity;
        }

        @Override
        protected Map<String, Field> run() throws Exception {
            return executeStrategy(fig.getReadCL());
        }

        @Override
        protected Map<String, Field> getFallback() {
            return executeStrategy(fig.getConsistentReadCL());
        }

        public Map<String, Field> executeStrategy(ConsistencyLevel consistencyLevel){
            //allocate our max size, worst case
            //now get the set of fields back
            final UniqueValueSet uniqueValues;

            final CollectionMembers<Field>
                collectionMembers = new CollectionMembersImpl<Field>( collectionScope, uniqueFields );

            try {

             uniqueValues = uniqueValueSerializationStrategy.load( applicationScope ,consistencyLevel,
                    Collections.singleton( collectionMembers ));
            }
            catch ( ConnectionException e ) {
                throw new RuntimeException( "Unable to read from cassandra", e );
            }

            final Map<String, Field> uniquenessViolations = new HashMap<>( uniqueFields.size() );

            //loop through each field that was unique
            for ( final Field field : uniqueFields ) {


                final UniqueValue uniqueValue = uniqueValues.getValue( collectionScope, field.getName() );

                if ( uniqueValue == null ) {
                    throw new RuntimeException(
                        String.format( "Could not retrieve unique value for field %s, unable to verify",
                            field.getName() ) );
                }

                final Id returnedEntityId = uniqueValue.getEntityId();

                if ( !entity.getId().equals(returnedEntityId) ) {
                    uniquenessViolations.put( field.getName(), field );
                }
            }

            return uniquenessViolations;
        }
    }

    /**
     * Command group used for realtime user commands
     */
    public static final HystrixCommand.Setter
        REPLAY_GROUP = HystrixCommand.Setter.withGroupKey(
            HystrixCommandGroupKey.Factory.asKey( "user" ) ).andThreadPoolPropertiesDefaults(
                HystrixThreadPoolProperties.Setter().withCoreSize( 1000 ) );
}
