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
#include "concallbacks.h"


#include <log4cpp/Category.hh>

static log4cpp::Category &LOG = log4cpp::Category::getInstance("hedwig."__FILE__);

using namespace Hedwig;

const int SUBSCRIBER_RECONNECT_TIME = 3000; // 3 seconds

SubscribeConnectCallback::SubscribeConnectCallback(SubscriberImpl& subscriber, const PubSubDataPtr& data, 
                                        	   const SubscriberClientChannelHandlerPtr& handler)
  : subscriber(subscriber), data(data), handler(handler) {
}

void SubscribeConnectCallback::channelConnected(const DuplexChannelPtr& channel) {
  subscriber.doSubscribe(channel, data, handler);
}

void SubscribeConnectCallback::channelError(const DuplexChannelPtr& channel, const std::exception& e) {
  data->getCallback()->operationFailed(e);
}

PublishConnectCallback::PublishConnectCallback(PublisherImpl& publisher, const PubSubDataPtr& data)
  : publisher(publisher), data(data) {
}

void PublishConnectCallback::channelConnected(const DuplexChannelPtr& channel) {
  publisher.doPublish(channel, data);
}

void PublishConnectCallback::channelError(const DuplexChannelPtr& channel, const std::exception& e) {
  data->getCallback()->operationFailed(e);
}


UnsubscribeConnectCallback::UnsubscribeConnectCallback(SubscriberImpl& subscriber, const PubSubDataPtr& data) 
  : subscriber(subscriber), data(data) {
}

void UnsubscribeConnectCallback::channelConnected(const DuplexChannelPtr& channel) {
  subscriber.doUnsubscribe(channel, data);
}

void UnsubscribeConnectCallback::channelError(const DuplexChannelPtr& channel, const std::exception& e) {
  data->getCallback()->operationFailed(e);
}

ReconnectSubscribeConnectCallback::ReconnectSubscribeConnectCallback(const ClientImplPtr& client, 
								     const PubSubDataPtr& data, 
								     const SubscriberClientChannelHandlerPtr& oldhandler,
								     const SubscriberClientChannelHandlerPtr& newhandler) 
  : client(client), data(data), oldhandler(oldhandler), newhandler(newhandler) {
}

void ReconnectSubscribeConnectCallback::channelConnected(const DuplexChannelPtr& channel) {
  oldhandler->handoverDelivery(newhandler);
  
  // remove record of the failed channel from the subscriber
  client->getSubscriberImpl().closeSubscription(data->getTopic(), data->getSubscriberId());
  
  // subscriber
  client->getSubscriberImpl().doSubscribe(channel, data, newhandler);
}

/*static*/ void ReconnectSubscribeConnectCallback::timerComplete(const SubscriberClientChannelHandlerPtr handler,
								 const DuplexChannelPtr channel, const std::exception e, 
								 const boost::system::error_code& error) {
  if (error) {
    return;
  }
  handler->channelDisconnected(channel, e);
}

void ReconnectSubscribeConnectCallback::channelError(const DuplexChannelPtr& channel, const std::exception& e) {
  if (typeid(e) == typeid(ShuttingDownException)) {
    client->getSubscriberImpl().closeSubscription(data->getTopic(), data->getSubscriberId());
    return;
  }
  int retrywait = client->getConfiguration().getReconnectSubscribeRetryWaitTime();

  boost::asio::deadline_timer t(client->getService(), boost::posix_time::milliseconds(retrywait));
  t.async_wait(boost::bind(&ReconnectSubscribeConnectCallback::timerComplete, oldhandler, 
			   channel, e, boost::asio::placeholders::error));  
}
