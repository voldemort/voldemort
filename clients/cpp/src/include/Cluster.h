/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file Cluster.h
 * @brief Interface definition file for Cluster
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

#ifndef CLUSTER_H
#define CLUSTER_H

#include "Node.h"

#include <string>
#include <map>
#include <list>
#include <boost/shared_ptr.hpp>
#include <expat.h>
#include <sstream>

namespace Voldemort {
   
using namespace boost;

class Cluster;

/**
 * Represent a Voldemort cluster configuration
 */
class Cluster
{
public:
    /**
     * Construct a new Cluster object from an XML string
     * 
     * @param clusterXml the Xml string
     */
    explicit Cluster(const std::string clusterXml);
    ~Cluster() {}

    /**
     * Get the specified node by node ID
     * 
     * @param nodeId the node Id
     * @return pointer to the node
     */
    boost::shared_ptr<Node>& getNodeById(int nodeId);

    /**
     * Get the number of nodes
     *
     * @return the number of nodes
     */
    int getNodeCount() { return nodesById.size(); }

    /**
     * Type used for node map
     */
    typedef std::map<int, boost::shared_ptr<Node> > nodeMap;

    /**
     * Get the node map
     * 
     * @return the node map
     */
    const nodeMap* getNodeMap() 
    { return &nodesById; }

    /** 
     * Stream insertion operator for cluster 
     *
     * @param output the stream
     * @param cluster the cluster object
     * @return the stream
     */
    friend std::ostream& operator<<(std::ostream& output, const Cluster& cluster);

private:
    static void startElement(void* data, const XML_Char*el, const XML_Char **attr);
    static void endElement(void* data, const XML_Char *el);
    static void charData(void* data, const XML_Char *s, int len);

    std::string name;
    int numberOfTags;
    std::map<int, boost::shared_ptr<Node> > nodesById;

    int state;
    std::stringstream buffer;
    boost::shared_ptr<Node> curNode;
};


} /* namespace Voldemort */

#endif /* CLUSTER_H */
