/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <hedwig/client.h>
#include <memory>

#include "clientimpl.h"
#include <log4cpp/Category.hh>

static log4cpp::Category &LOG = log4cpp::Category::getInstance("hedwig."__FILE__);

using namespace Hedwig;

const std::string DEFAULT_SERVER = "localhost:4080";
const std::string& Configuration::getDefaultServer() const {
  return DEFAULT_SERVER;
}

Client::Client(const Configuration& conf) {
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "Client::Client (" << this << ")";
  }
  clientimpl = ClientImpl::Create( conf );
}

Subscriber& Client::getSubscriber() {
  return clientimpl->getSubscriber();
}

Publisher& Client::getPublisher() {
  return clientimpl->getPublisher();
}

Client::~Client() {
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "Client::~Client (" << this << ")";
  }

  clientimpl->Destroy();
}


