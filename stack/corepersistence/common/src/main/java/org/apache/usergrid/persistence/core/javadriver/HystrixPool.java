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


import com.datastax.driver.core.Session;


/**
 * Operations to get pools of connections based on the customer.  Only 1 instance of this implementation should exist per JVM
 *
 */
public interface HystrixPool {


    /**
     * Get a restricted session for the given identifier.  Identifier must be a string that uniquely identifies a
     * set of customer constrained connections.
     *
     * @param identifier
     * @return
     */
    public Session getSession( String identifier );


    /**
     * Close the customer connection pool
     */
    public void close();


}
