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

package org.apache.usergrid.persistence.core.javadriver;


import java.util.Collection;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;


/**
 * Utility class for rolling up multiple statements as a batch
 *
 */
public class BatchStatementUtils {


    /**
     * Roll up the statements into batches (non atomic) and execute them
     * @param session
     * @param statements
     */
    public static ResultSet runBatches( final Session session,
                                                                 Collection<? extends Statement>... statements ){


        final BatchStatement batch = fastStatement();

        for(Collection<? extends Statement> statement: statements){
            batch.addAll( statement );
        }


        return session.execute( batch );
    }


    /**
     * We're running on an eventually consistent system, don't use logged statement (paxos & atomic) unless you
     * absolutely must!
     *
     * @return
     */
    public static BatchStatement fastStatement(){
        return new BatchStatement( BatchStatement.Type.UNLOGGED );
    }



}
