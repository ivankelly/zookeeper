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
#ifndef HEDWIG_CONCALLBACKS_H
#define HEDWIG_CONCALLBACKS_H

#include "publisherimpl.h"
#include "subscriberimpl.h"
#include "channel.h"

namespace Hedwig {

  class SubscribeConnectCallback : public ChannelConnectCallback {
  public:
    //  getSubscriberImpl().doSubscribe(newchannel, data, handler);
    SubscribeConnectCallback(SubscriberImpl& subscriber, const PubSubDataPtr& data, 
			     const SubscriberClientChannelHandlerPtr& handler);
    virtual void channelConnected(const DuplexChannelPtr& channel);
    virtual void channelError(const DuplexChannelPtr& channel, const std::exception& e);

  private:
    SubscriberImpl& subscriber; 
    const PubSubDataPtr data;
    const SubscriberClientChannelHandlerPtr handler;
  };

  class PublishConnectCallback : public ChannelConnectCallback {
  public:
    // getPublisherImpl().doPublish(newchannel, data);
    PublishConnectCallback(PublisherImpl& publisher, const PubSubDataPtr& data);
    virtual void channelConnected(const DuplexChannelPtr& channel);
    virtual void channelError(const DuplexChannelPtr& channel, const std::exception& e);
  private:
    PublisherImpl& publisher;
    const PubSubDataPtr data;
  };

  class UnsubscribeConnectCallback : public ChannelConnectCallback {
  public:
    //    getSubscriberImpl().doUnsubscribe(newchannel, data);
    UnsubscribeConnectCallback(SubscriberImpl& subscriber, const PubSubDataPtr& data);
    virtual void channelConnected(const DuplexChannelPtr& channel);
    virtual void channelError(const DuplexChannelPtr& channel, const std::exception& e);
  private:
    SubscriberImpl& subscriber;
    const PubSubDataPtr data;
  };

  class ReconnectSubscribeConnectCallback : public ChannelConnectCallback {
  public:
    ReconnectSubscribeConnectCallback(const ClientImplPtr& client, const PubSubDataPtr& data, 
				      const SubscriberClientChannelHandlerPtr& oldhandler, 
				      const SubscriberClientChannelHandlerPtr& newhandler);
    virtual void channelConnected(const DuplexChannelPtr& channel);
    virtual void channelError(const DuplexChannelPtr& channel, const std::exception& e);
    
    static void timerComplete(SubscriberClientChannelHandlerPtr handler, const DuplexChannelPtr channel, 
			      const std::exception e, const boost::system::error_code& error);

  private:
    const ClientImplPtr client;
    const PubSubDataPtr data;
    const SubscriberClientChannelHandlerPtr oldhandler;
    const SubscriberClientChannelHandlerPtr newhandler;
  };
}


#endif
