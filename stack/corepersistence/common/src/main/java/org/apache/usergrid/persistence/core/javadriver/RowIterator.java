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


import java.util.Iterator;
import java.util.NoSuchElementException;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.query.RowQuery;


/**
 * Simple iterator that wraps a Row query and will keep executing it's paging until there are no more results to read
 * from cassandra
 */
public class RowIterator<T> implements Iterable<T>, Iterator<T> {


    private final RowParser<T> parser;
    private final Session session;
    private final Statement statement;

    private Iterator<Row> iterator;



    public RowIterator( final Session session, final Statement statement, final RowParser<T> parser ) {
        this.session = session;
        this.statement = statement;
        this.parser = parser;
    }


    @Override
    public Iterator<T> iterator() {
        return this;
    }


    @Override
    public boolean hasNext() {

        if ( iterator == null ) {
          startIterator();
        }

        return iterator.hasNext();
    }


    @Override
    public T next() {

        if ( !hasNext() ) {
            throw new NoSuchElementException();
        }

        return parser.parseRow( iterator.next());
    }


    @Override
    public void remove() {
       throw new UnsupportedOperationException( "Remove is unsupported" );
    }


    /**
     * Execute the query again and set the results
     */
    private void startIterator() {

        iterator = session.execute( statement ).iterator();
    }
}
