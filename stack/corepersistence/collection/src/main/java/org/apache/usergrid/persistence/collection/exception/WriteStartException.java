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
package org.apache.usergrid.persistence.collection.exception;

import org.apache.usergrid.persistence.collection.CollectionScope;
import org.apache.usergrid.persistence.collection.MvccEntity;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;


public class WriteStartException extends CollectionRuntimeException {


    public WriteStartException( final MvccEntity entity, final ApplicationScope applicationScope,
                                final CollectionScope collectionScope, final String message ) {
        super( entity, applicationScope, collectionScope, message );
    }


    public WriteStartException( final MvccEntity entity, final ApplicationScope applicationScope,
                                final CollectionScope collectionScope, final String message, final Throwable cause ) {
        super( entity, applicationScope, collectionScope, message, cause );
    }


    public WriteStartException( final MvccEntity entity, final ApplicationScope applicationScope,
                                final CollectionScope collectionScope, final Throwable cause ) {
        super( entity, applicationScope, collectionScope, cause );
    }


    public WriteStartException( final MvccEntity entity, final ApplicationScope applicationScope,
                                final CollectionScope collectionScope, final String message, final Throwable cause,
                                final boolean enableSuppression, final boolean writableStackTrace ) {
        super( entity, applicationScope, collectionScope, message, cause, enableSuppression, writableStackTrace );
    }
}
