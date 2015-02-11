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

package org.apache.usergrid.rest.management.organizations.applications.imports;


import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import org.apache.amber.oauth2.common.exception.OAuthSystemException;
import org.apache.amber.oauth2.common.message.OAuthResponse;

import org.apache.usergrid.management.ApplicationInfo;
import org.apache.usergrid.management.OrganizationInfo;
import org.apache.usergrid.management.importer.ImportService;
import org.apache.usergrid.persistence.index.query.Identifier;
import org.apache.usergrid.persistence.queue.impl.UsergridAwsCredentials;
import org.apache.usergrid.rest.AbstractContextResource;
import org.apache.usergrid.rest.RootResource;
import org.apache.usergrid.rest.applications.ServiceResource;
import org.apache.usergrid.rest.applications.users.UserResource;
import org.apache.usergrid.rest.security.annotations.RequireOrganizationAccess;
import org.apache.usergrid.rest.utils.JSONPUtils;

import com.amazonaws.AmazonClientException;

import static javax.servlet.http.HttpServletResponse.SC_ACCEPTED;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import static org.apache.usergrid.services.ServiceParameter.addParameter;


@Component("org.apache.usergrid.rest.management.organizations.applications.ImportResource")
@Scope("prototype")
@Produces(MediaType.APPLICATION_JSON)
public class ImportResource extends ServiceResource {


    private Identifier importId;

    /**
     * Override our service manager factory so that we get entities from the root management app
     */
    public ImportResource() {
        //override the services management app

    }

    public ImportResource init(final Identifier importId){
        this.importId = importId;
        services = smf.getServiceManager( emf.getManagementAppId() );
        return this;
    }




}
