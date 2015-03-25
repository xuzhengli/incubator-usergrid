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


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.usergrid.persistence.model.entity.Id;


/**
 * A set that contains the collection scope and the ids
 */
public class CollectionMembers<T> {


    public final CollectionScope scope;
    public final Collection<T> identifiers;


    public CollectionMembers( final CollectionScope scope, final Collection<T> identifiers ) {
        this.scope = scope;
        this.identifiers = identifiers;
    }


    public CollectionMembers( final CollectionScope scope, final T identifier ) {
        this.scope = scope;
        this.identifiers = Collections.singleton( identifier );
    }



    public CollectionMembers( final CollectionScope scope ) {
        this.scope = scope;
        this.identifiers = new ArrayList<>(  );
    }


    public CollectionScope getScope() {
        return scope;
    }


    public Collection<T> getIdentifiers() {
        return identifiers;
    }


    public void addIdentifier( final T identifier ) {
        this.identifiers.add( identifier );
    }
}
