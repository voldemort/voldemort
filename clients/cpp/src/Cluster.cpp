/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Implementation for Cluster class.
 * 
 * Copyright (c) 2009 Webroot Software, Inc.
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

#include "Cluster.h"
#include <voldemort/VoldemortException.h>
#include <iostream>
#include <string.h>

namespace Voldemort {

using namespace boost;

enum STATE {
    STATE_BEGIN,
    STATE_CLUSTER,
    STATE_NAME,
    STATE_SERVER,
    STATE_ID,
    STATE_HOST,
    STATE_HTTP_PORT,
    STATE_SOCKET_PORT,
    STATE_ADMIN_PORT,
    STATE_PARTITIONS
};

void Cluster::startElement(void* data, const XML_Char*el, const XML_Char **attr) {
    Cluster* cl = (Cluster*) data;

    if (cl->state == STATE_BEGIN && 0 == strcasecmp(el, "cluster")) {
        cl->state = STATE_CLUSTER;
    } else if (cl->state == STATE_CLUSTER && 0 == strcasecmp(el, "name")) {
        cl->state = STATE_NAME;
        cl->buffer.str("");
        cl->buffer.clear();
    } else if (cl->state == STATE_CLUSTER && 0 == strcasecmp(el, "server")) {
        cl->state = STATE_SERVER;
        cl->curNode = shared_ptr<Node>(new Node());
    } else if (cl->state == STATE_SERVER && 0 == strcasecmp(el, "id")) {
        cl->state = STATE_ID;
        cl->buffer.str("");
        cl->buffer.clear();
    } else if (cl->state == STATE_SERVER && 0 == strcasecmp(el, "host")) {
        cl->state = STATE_HOST;
        cl->buffer.str("");
        cl->buffer.clear();
    } else if (cl->state == STATE_SERVER && 0 == strcasecmp(el, "http-port")) {
        cl->state = STATE_HTTP_PORT;
        cl->buffer.str("");
        cl->buffer.clear();
    } else if (cl->state == STATE_SERVER && 0 == strcasecmp(el, "socket-port")) {
        cl->state = STATE_SOCKET_PORT;
        cl->buffer.str("");
        cl->buffer.clear();
    } else if (cl->state == STATE_SERVER && 0 == strcasecmp(el, "admin-port")) {
        cl->state = STATE_ADMIN_PORT;
        cl->buffer.str("");
        cl->buffer.clear();
    } else if (cl->state == STATE_SERVER && 0 == strcasecmp(el, "partitions")) {
        cl->state = STATE_PARTITIONS;
        cl->buffer.str("");
        cl->buffer.clear();
    }
}

void Cluster::endElement(void* data, const XML_Char *el) {
    Cluster* cl = (Cluster*) data;

    if (cl->state == STATE_CLUSTER && 0 == strcasecmp(el, "cluster")) {
        cl->state = STATE_BEGIN;
    } else if (cl->state == STATE_NAME && 0 == strcasecmp(el, "name")) {
        cl->state = STATE_CLUSTER;
        cl->name = cl->buffer.str();
    } else if (cl->state == STATE_SERVER && 0 == strcasecmp(el, "server")) {
        cl->state = STATE_CLUSTER;

        if (cl->curNode->id_ < 0) 
            throw VoldemortException("server element missing ID");
        if (cl->curNode->host_.length() == 0) 
            throw VoldemortException("server element missing hostname");
        if (cl->curNode->socketPort_ == 0) 
            throw VoldemortException("server element missing socket port");

        cl->nodesById[cl->curNode->id_] = cl->curNode;
        cl->curNode.reset();

    } else if (cl->state == STATE_ID && 0 == strcasecmp(el, "id")) {
        cl->state = STATE_SERVER;
        cl->buffer >> cl->curNode->id_;
    } else if (cl->state == STATE_HOST && 0 == strcasecmp(el, "host")) {
        cl->state = STATE_SERVER;
        cl->curNode->host_ = cl->buffer.str();
    } else if (cl->state == STATE_HTTP_PORT && 0 == strcasecmp(el, "http-port")) {
        cl->state = STATE_SERVER;
        cl->buffer >> cl->curNode->httpPort_;
    } else if (cl->state == STATE_SOCKET_PORT && 0 == strcasecmp(el, "socket-port")) {
        cl->state = STATE_SERVER;
        cl->buffer >> cl->curNode->socketPort_;
    } else if (cl->state == STATE_ADMIN_PORT && 0 == strcasecmp(el, "admin-port")) {
        cl->state = STATE_SERVER;
        cl->buffer >> cl->curNode->adminPort_;
    } else if (cl->state == STATE_PARTITIONS && 0 == strcasecmp(el, "partitions")) {
        cl->state = STATE_SERVER;
        do {
            int part = -1;
            cl->buffer >> part;

            if (part >= 0 && !cl->buffer.fail()) {
                cl->curNode->partitions_->push_back(part);

                if (cl->buffer.eof()) break;
                int p = cl->buffer.peek();
                while (p == ',' || p == ' ' || p == '\t') {
                    cl->buffer.get();
                    p = cl->buffer.peek();
                }
            }
        } while (!cl->buffer.eof() && !cl->buffer.fail());

        if (cl->buffer.fail()) 
            throw VoldemortException("Failed to parse partition list");
    }

}

void Cluster::charData(void* data, const XML_Char *s, int len) {
    Cluster* cl = (Cluster*) data;

    switch (cl->state) {
    case STATE_NAME:
    case STATE_ID:
    case STATE_HOST:
    case STATE_SOCKET_PORT:
    case STATE_ADMIN_PORT:
    case STATE_HTTP_PORT:
    case STATE_PARTITIONS:
        cl->buffer.write(s, len);
        break;
    }

}

Cluster::Cluster(const std::string clusterXml)
    : state(STATE_BEGIN) {
    XML_Parser parser = NULL;
    try {
        XML_Status ec;
        parser = XML_ParserCreate(NULL);
        XML_SetUserData(parser, this);
        XML_SetElementHandler(parser, 
                              &Voldemort::Cluster::startElement, 
                              &Voldemort::Cluster::endElement);
        XML_SetCharacterDataHandler(parser,
                                    &Voldemort::Cluster::charData);
        if (!(ec = XML_Parse(parser, clusterXml.c_str(), clusterXml.length(), 1))) {
            throw VoldemortException("Failed to parse cluster XML data: " +
                                     std::string(XML_ErrorString(XML_GetErrorCode(parser))));
        }
        XML_ParserFree(parser);
        parser = NULL;
    } catch (...) {
        if (parser != NULL) XML_ParserFree(parser);
        throw;
    }
}

boost::shared_ptr<Node>& Cluster::getNodeById(int nodeId) {
    if (nodesById.count(nodeId))
        return nodesById[nodeId];
    else 
        throw VoldemortException("Invalid node ID: " + nodeId);
}

std::ostream& operator<<(std::ostream& output, const Cluster& cluster) {
    output << "Cluster('" << cluster.name <<  "', [";
    std::map<int, boost::shared_ptr<Node> >::const_iterator it;
    for (it = cluster.nodesById.begin(); it != cluster.nodesById.end(); ++it) {
        if (it != cluster.nodesById.begin()) output << ", ";
        output << *(it->second);
    }
    output << "])";
    return output;
}

} /* namespace Voldemort */
