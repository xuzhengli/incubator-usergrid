/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.usergrid.persistence.model.entity.Id;


/**
 * A set that contains the collection scope and the ids
 */
public interface ScopeSet<T> {

    /**
     * Get the scope for this set of ids
     * @return
     */
    CollectionScope getScope();

    /**
     * Get the set of Ids to load for this scope
     * @return
     */
    Collection<T> getIdentifiers();

    /**
     * Add the itentifier to the list
     * @param identifier
     */
    void addIdentifier(T identifier);
}
