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
package org.apache.usergrid.persistence.collection;


import java.util.Collection;
import org.apache.usergrid.persistence.core.util.Health;
import org.apache.usergrid.persistence.model.entity.Entity;
import org.apache.usergrid.persistence.model.entity.Id;
import org.apache.usergrid.persistence.model.field.Field;
import rx.Observable;


/**
 * The operations for performing changes on an entity
 */
public interface EntityCollectionManager {

    /**
     * Write the entity in the entity collection.  This is an entire entity, it's contents will
     * completely overwrite the previous values, if it exists.
     * @param entity The entity to update
     */
    Observable<Entity> write( CollectionScope collectionScope, Entity entity );


    /**
     * MarkCommit the entity and remove it's indexes with the given entity id
     * @param collectionScope The scope this enttity exists in
     * @param entityId The entity id to delete
     */
    Observable<Id> delete( CollectionScope collectionScope, Id entityId );

    /**
     * Load the entity with the given entity Id
     * @param collectionScope The scope this entity exists in
     * @param entityId The entity id to remove
     */
    Observable<Entity> load(CollectionScope collectionScope,  Id entityId );

    /**
     * Return the latest versions of the specified entityIds
     *
     * @param entityId A collection of scopes with the entity Ids set
     */
    Observable<VersionSet> getLatestVersion( Collection<ScopeSet<Id>> entityId );


    /**
     *
     * @param fields The collection of scopes for fields to use
     * @return
     */
    Observable<FieldSet> getEntitiesFromFields( Collection<ScopeSet<Field>> fields );

    /**
     * Gets the Id for a field
     *
     * @param collectionScope The scope for the field
     * @param field The field for the scope
     *
     * @return most likely a single Id, watch for onerror events
     */
    Observable<Id> getIdField(CollectionScope collectionScope,  Field field);

    /**
     * Load all the entityIds into the observable entity set
     *
     * @param entityIds The entity ids
     */
    Observable<EntitySet> load(Collection<ScopeSet<Id>> entityIds);


    /**
     * Returns health of entity data store.
     */
    Health getHealth();

}
