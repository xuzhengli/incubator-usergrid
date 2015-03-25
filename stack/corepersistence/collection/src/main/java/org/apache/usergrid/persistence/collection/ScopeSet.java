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


import java.util.HashMap;
import java.util.Map;


/**
 * A set of scopes, keyed by collectionScope.
 *
 * @param <T> The type of value to encapsulate within the scope.  Examples would be Id objects or Fields
 */
public class ScopeSet<T> {

    private Map<CollectionScope, CollectionMembers<T>> members  = new HashMap<>(  );


    /**
     * A factory method to generate a collection member with the given scope.  If one exists, it will be returned
     * @param scope
     * @return
     */
    public CollectionMembers<T> getMembers(final CollectionScope scope){
        final CollectionMembers<T> existing = members.get( scope );

        if(existing != null){
            return existing;
        }

        final CollectionMembers<T> newInstance = new CollectionMembers<T>( scope );

        members.put( scope, newInstance );

        return newInstance;
    }



}
