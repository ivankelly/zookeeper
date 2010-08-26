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

#ifndef HEDWIG_CLIENT_IMPL_H
#define HEDWIG_CLIENT_IMPL_H

#include <hedwig/client.h>
#include <hedwig/protocol.h>

#include <boost/asio.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>

#include <tr1/unordered_map>
#include <list>

#include "util.h"
#include "channel.h"
#include "data.h"
#include "eventdispatcher.h"

namespace Hedwig {
  class SyncOperationCallback : public OperationCallback, public WaitConditionBase {
  public:
    SyncOperationCallback() : response(PENDING) {}
    virtual void operationComplete();
    virtual void operationFailed(const std::exception& exception);
    
    virtual bool isTrue();

    void throwExceptionIfNeeded();
    
  private:
    enum { 
      PENDING, 
      SUCCESS,
      NOCONNECT,
      SERVICEDOWN,
      NOT_SUBSCRIBED,
      ALREADY_SUBSCRIBED,
      UNKNOWN
    } response;
  };

  class HedwigClientChannelHandler : public ChannelHandler {
  public:
    HedwigClientChannelHandler(const ClientImplPtr& client);
    
    virtual void messageReceived(const DuplexChannelPtr& channel, const PubSubResponsePtr& m);
    virtual void channelConnected(const DuplexChannelPtr& channel);
    virtual void channelDisconnected(const DuplexChannelPtr& channel, const std::exception& e);
    virtual void exceptionOccurred(const DuplexChannelPtr& channel, const std::exception& e);
    
  protected:
    const ClientImplPtr client;
  };
  
  class PublisherImpl;
  class SubscriberImpl;
  
  /**
     Implementation of the hedwig client. This class takes care of globals such as the topic->host map and the transaction id counter.
  */
  class ClientImpl : public boost::enable_shared_from_this<ClientImpl> {
  public:
    static ClientImplPtr Create(const Configuration& conf);
    void Destroy();

    Subscriber& getSubscriber();
    Publisher& getPublisher();

    ClientTxnCounter& counter();

    void redirectRequest(const DuplexChannelPtr& channel, PubSubDataPtr& data, const PubSubResponsePtr& response);

    const HostAddress& getHostForTopic(const std::string& topic);

    //DuplexChannelPtr getChannelForTopic(const std::string& topic, OperationCallback& callback);
    //DuplexChannelPtr createChannelForTopic(const std::string& topic, ChannelHandlerPtr& handler, OperationCallback& callback);
    DuplexChannelPtr withNewChannel(const std::string& topic, const ChannelHandlerPtr& handler, const ChannelConnectCallbackPtr& callback);    
    DuplexChannelPtr withChannel(const std::string& topic, const ChannelConnectCallbackPtr& callback);

    void setHostForTopic(const std::string& topic, const HostAddress& host);

    void setChannelForHost(const HostAddress& address, const DuplexChannelPtr& channel);
    void channelDied(const DuplexChannelPtr& channel);
    bool shuttingDown() const;
    
    SubscriberImpl& getSubscriberImpl();
    PublisherImpl& getPublisherImpl();

    const Configuration& getConfiguration();
    boost::asio::io_service& getService();

    ~ClientImpl();
  private:
    ClientImpl(const Configuration& conf);

    const Configuration& conf;

    Mutex publishercreate_lock;
    PublisherImpl* publisher;

    Mutex subscribercreate_lock;
    SubscriberImpl* subscriber;

    ClientTxnCounter counterobj;

    EventDispatcher dispatcher;
    
    typedef std::tr1::unordered_multimap<HostAddress, std::string, HostAddressHash > Host2TopicsMap;
    Host2TopicsMap host2topics;
    Mutex host2topics_lock;

    std::tr1::unordered_map<HostAddress, DuplexChannelPtr, HostAddressHash > host2channel;
    Mutex host2channel_lock;
    std::tr1::unordered_map<std::string, HostAddress> topic2host;
    Mutex topic2host_lock;

    typedef std::tr1::unordered_set<DuplexChannelPtr, DuplexChannelPtrHash > ChannelMap;
    ChannelMap allchannels;
    Mutex allchannels_lock;

    bool shuttingDownFlag;
  };
};
#endif
