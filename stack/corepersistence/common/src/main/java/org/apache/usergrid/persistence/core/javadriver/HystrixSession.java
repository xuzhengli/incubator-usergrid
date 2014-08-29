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


import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixThreadPoolProperties;


/**
 * Create a session that delegates execute methods via hystrix
 */
public class HystrixSession implements Session {

    private final Session delegate;


    private final HystrixCommand.Setter commandSetter;


    public HystrixSession( final Session delegate, final String serviceName, final String organization,
                           int defaultSize ) {
        this.delegate = delegate;

        final String keyName = serviceName + "/" + organization;

        final HystrixCommandGroupKey groupKey = HystrixCommandGroupKey.Factory.asKey( keyName );



        commandSetter = HystrixCommand.Setter.withGroupKey( groupKey ).andThreadPoolPropertiesDefaults( HystrixThreadPoolProperties

                .Setter()
                   .withCoreSize(defaultSize) );
    }


    @Override
    public String getLoggedKeyspace() {
        return delegate.getLoggedKeyspace();
    }


    @Override
    public Session init() {
        return delegate.init();
    }


    @Override
    public ResultSet execute( final String query ) {
        return new HystrixCommand<ResultSet>( commandSetter ) {

            @Override
            protected ResultSet run() throws Exception {
                return delegate.execute( query );
            }
        }.execute();
    }


    @Override
    public ResultSet execute( final String query, final Object... values ) {
        return new HystrixCommand<ResultSet>( commandSetter ) {

            @Override
            protected ResultSet run() throws Exception {
                return delegate.execute( query, values );
            }
        }.execute();
    }


    @Override
    public ResultSet execute( final Statement statement ) {
        return new HystrixCommand<ResultSet>( commandSetter ) {

            @Override
            protected ResultSet run() throws Exception {
                return delegate.execute( statement );
            }
        }.execute();
    }


    /**
     * Once hystrix 1.4 with full async support comes out we can enable this
     */
    @Override
    public ResultSetFuture executeAsync( final String query ) {
        throw new UnsupportedOperationException( "Asynchronous execute is unsupported" );
    }


    @Override
    public ResultSetFuture executeAsync( final String query, final Object... values ) {
        throw new UnsupportedOperationException( "Asynchronous execute is unsupported" );
    }


    @Override
    public ResultSetFuture executeAsync( final Statement statement ) {
        throw new UnsupportedOperationException( "Asynchronous execute is unsupported" );
    }


    @Override
    public PreparedStatement prepare( final String query ) {
        return delegate.prepare( query );
    }


    @Override
    public PreparedStatement prepare( final RegularStatement statement ) {
        return delegate.prepare( statement );
    }


    @Override
    public ListenableFuture<PreparedStatement> prepareAsync( final String query ) {
        return delegate.prepareAsync( query );
    }


    @Override
    public ListenableFuture<PreparedStatement> prepareAsync( final RegularStatement statement ) {
        return delegate.prepareAsync( statement );
    }


    @Override
    public CloseFuture closeAsync() {
        return delegate.closeAsync();
    }


    @Override
    public void close() {
        delegate.close();
    }


    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }


    @Override
    public Cluster getCluster() {
        return delegate.getCluster();
    }


    @Override
    public State getState() {
        return delegate.getState();
    }
}
