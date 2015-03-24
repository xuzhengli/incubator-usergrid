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


import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.model.entity.Id;


/**
 * A scope to use when creating the collection manager.  Typically, this would be something like an application, or an
 * organization. Data encapsulated within instances of a scope are mutually exclusive from instances with other ids and
 * names.
 */
public interface CollectionScope  {

    /**
     * @return The name of the collection. If you use pluralization for you names vs types,
     * you must keep the consistent or you will be unable to load data
     */
    String getName();


    /**
     * @return A uuid that is unique to this context.  It can be any uuid (time uuid preferred).  Usually an application
     *         Id, but could be an entity Id that is the parent of another collection
     */
    Id getOwner();
}
