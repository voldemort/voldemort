/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file ClientConfig.h
 * @brief Interface definition file for ClientConfig
 */
/* Copyright (c) 2009 Webroot Software, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

#ifndef CLIENTCONFIG_H
#define CLIENTCONFIG_H

#include <list>
#include <string>

namespace Voldemort {

/**
 * Implementation details for ClientConfig
 */
class ClientConfigImpl;

/**
 * A configuration object that holds configuration parameters for the
 * client
 */
class ClientConfig
{
 public:
    ClientConfig();
    /** Copy constructor */
    ClientConfig(const ClientConfig& cc);
    ~ClientConfig();

    /** 
     * Sets the list of bootstrap URLs to use to attempt a connection.
     * This list is not copied and the memory is owned by the caller.
     * There is no default list.
     * 
     * @param bootstrapUrls the list of URLs to use
     * @return a pointer to this object useful for chaining
     */
    ClientConfig* setBootstrapUrls(std::list<std::string>* bootstrapUrls);

    /**
     * Return the current list of bootstrap URLs.
     * 
     * @return pointer to the list set using @ref setBootstrapUrls
     */
    std::list<std::string>* getBootstrapUrls();

    /* XXX TODO add getters/setters for other options */

 private:
    /** Internal implementation details for ClientConfig */
    ClientConfigImpl* pimpl_;
};

} /* namespace Voldemort */

#endif /* CLIENTCONFIG_H */

