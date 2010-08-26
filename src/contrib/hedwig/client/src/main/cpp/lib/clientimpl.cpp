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
#include "clientimpl.h"
#include "channel.h"
#include "publisherimpl.h"
#include "subscriberimpl.h"
#include <log4cpp/Category.hh>
#include "concallbacks.h"

static log4cpp::Category &LOG = log4cpp::Category::getInstance("hedwig."__FILE__);

using namespace Hedwig;

void SyncOperationCallback::operationComplete() {
  lock();
  response = SUCCESS;
  signalAndUnlock();
}

void SyncOperationCallback::operationFailed(const std::exception& exception) {
  lock();
  if (typeid(exception) == typeid(ChannelConnectException)) {
    response = NOCONNECT;
  } else if (typeid(exception) == typeid(ServiceDownException)) {
    response = SERVICEDOWN;
  } else if (typeid(exception) == typeid(AlreadySubscribedException)) {
    response = ALREADY_SUBSCRIBED;
  } else if (typeid(exception) == typeid(NotSubscribedException)) {
    response = NOT_SUBSCRIBED;
  } else {
    response = UNKNOWN;
  }
  signalAndUnlock();
}
    
bool SyncOperationCallback::isTrue() {
  return response != PENDING;
}

void SyncOperationCallback::throwExceptionIfNeeded() {
  switch (response) {
  case SUCCESS:
    break;
  case NOCONNECT:
    throw CannotConnectException();
    break;
  case SERVICEDOWN:
    throw ServiceDownException();
    break;
  case ALREADY_SUBSCRIBED:
    throw AlreadySubscribedException();
    break;
  case NOT_SUBSCRIBED:
    throw NotSubscribedException();
    break;
  default:
    throw ClientException();
    break;
  }
}

HedwigClientChannelHandler::HedwigClientChannelHandler(const ClientImplPtr& client) 
  : client(client){
}

void HedwigClientChannelHandler::messageReceived(const DuplexChannelPtr& channel, const PubSubResponsePtr& m) {
  LOG.debugStream() << "Message received";
  if (m->has_message()) {
    LOG.errorStream() << "Subscription response, ignore for now";
    return;
  }
  
  long txnid = m->txnid();
  PubSubDataPtr data = channel->retrieveTransaction(m->txnid()); 
  /* you now have ownership of data, don't leave this funciton without deleting it or 
     palming it off to someone else */

  if (data == NULL) {
    LOG.errorStream() << "Transaction " << m->txnid() << " doesn't exist in channel " << channel;
    return;
  }

  if (m->statuscode() == NOT_RESPONSIBLE_FOR_TOPIC) {
    client->redirectRequest(channel, data, m);
    return;
  }

  switch (data->getType()) {
  case PUBLISH:
    client->getPublisherImpl().messageHandler(m, data);
    break;
  case SUBSCRIBE:
  case UNSUBSCRIBE:
    client->getSubscriberImpl().messageHandler(m, data);
    break;
  default:
    LOG.errorStream() << "Unimplemented request type " << data->getType();
    break;
  }
}


void HedwigClientChannelHandler::channelConnected(const DuplexChannelPtr& channel) {
  // do nothing 
}

void HedwigClientChannelHandler::channelDisconnected(const DuplexChannelPtr& channel, const std::exception& e) {
  LOG.errorStream() << "Channel disconnected";

  client->channelDied(channel);
}

void HedwigClientChannelHandler::exceptionOccurred(const DuplexChannelPtr& channel, const std::exception& e) {
  LOG.errorStream() << "Exception occurred" << e.what();
}

ClientTxnCounter::ClientTxnCounter() : counter(0) 
{
}

ClientTxnCounter::~ClientTxnCounter() {
}

/**
Increment the transaction counter and return the new value.

@returns the next transaction id
*/
long ClientTxnCounter::next() {  // would be nice to remove lock from here, look more into it
  mutex.lock();
  long next= ++counter; 
  mutex.unlock();
  return next;
}

ClientImplPtr ClientImpl::Create(const Configuration& conf) {
  ClientImplPtr impl(new ClientImpl(conf));
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "Creating Clientimpl " << impl;
  }
  impl->dispatcher.start();

  return impl;
}

void ClientImpl::Destroy() {
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "destroying Clientimpl " << this;
  }
  allchannels_lock.lock();

  shuttingDownFlag = true;
  for (ChannelMap::iterator iter = allchannels.begin(); iter != allchannels.end(); ++iter ) {
    (*iter)->kill();
  }  
  allchannels.clear();
  allchannels_lock.unlock();
  /* destruction of the maps will clean up any items they hold */
  
  if (subscriber != NULL) {
    delete subscriber;
    subscriber = NULL;
  }
  if (publisher != NULL) {
    delete publisher;
    publisher = NULL;
  }
}

ClientImpl::ClientImpl(const Configuration& conf) 
  : conf(conf), subscriber(NULL), publisher(NULL), counterobj(), shuttingDownFlag(false)
{
}

Subscriber& ClientImpl::getSubscriber() {
  return getSubscriberImpl();
}

Publisher& ClientImpl::getPublisher() {
  return getPublisherImpl();
}
    
SubscriberImpl& ClientImpl::getSubscriberImpl() {
  if (subscriber == NULL) {
    subscribercreate_lock.lock();
    if (subscriber == NULL) {
      subscriber = new SubscriberImpl(shared_from_this());
    }
    subscribercreate_lock.unlock();
  }
  return *subscriber;
}

PublisherImpl& ClientImpl::getPublisherImpl() {
  if (publisher == NULL) { 
    publishercreate_lock.lock();
    if (publisher == NULL) {
      publisher = new PublisherImpl(shared_from_this());
    }
    publishercreate_lock.unlock();
  }
  return *publisher;
}

ClientTxnCounter& ClientImpl::counter() {
  return counterobj;
}

void ClientImpl::redirectRequest(const DuplexChannelPtr& channel, PubSubDataPtr& data, const PubSubResponsePtr& response) {
  HostAddress oldhost = channel->getHostAddress();
  data->addTriedServer(oldhost);
  
  HostAddress h = HostAddress::fromString(response->statusmsg());
  if (data->hasTriedServer(h)) {
    LOG.errorStream() << "We've been told to try request [" << data->getTxnId() << "] with [" << h.getAddressString()<< "] by " << channel->getHostAddress().getAddressString() << " but we've already tried that. Failing operation";
    data->getCallback()->operationFailed(InvalidRedirectException());
    return;
  }
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "We've been told  [" << data->getTopic() << "] is on [" << h.getAddressString() << "] by [" << oldhost.getAddressString() << "]. Redirecting request " << data->getTxnId();
  }
  data->setShouldClaim(true);

  setHostForTopic(data->getTopic(), h);
  DuplexChannelPtr newchannel;
  try {
    if (data->getType() == SUBSCRIBE) {
      SubscriberClientChannelHandlerPtr handler(new SubscriberClientChannelHandler(shared_from_this(), 
										   this->getSubscriberImpl(), data));
      ChannelConnectCallbackPtr connectcb(new SubscribeConnectCallback(getSubscriberImpl(), data, handler));
      newchannel = withNewChannel(data->getTopic(), handler, connectcb);
      handler->setChannel(newchannel);
    } else if (data->getType() == PUBLISH) {
      ChannelConnectCallbackPtr connectcb(new PublishConnectCallback(getPublisherImpl(), data));
      withChannel(data->getTopic(), connectcb);
    } else {
      ChannelConnectCallbackPtr connectcb(new UnsubscribeConnectCallback(getSubscriberImpl(), data));
      withChannel(data->getTopic(), connectcb);
    }
  } catch (ShuttingDownException& e) {
    return; // no point in redirecting if we're shutting down
  }
}

ClientImpl::~ClientImpl() {
  dispatcher.stop();
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "deleting Clientimpl " << this;
  }
}

DuplexChannelPtr ClientImpl::withNewChannel(const std::string& topic, const ChannelHandlerPtr& handler, 
					    const ChannelConnectCallbackPtr& callback) {
  // get the host address
  // create a channel to the host
  HostAddress addr = topic2host[topic];
  if (addr.isNullHost()) {
    addr = HostAddress::fromString(conf.getDefaultServer());
  }

  DuplexChannelPtr channel(new DuplexChannel(dispatcher, addr, conf, handler));

  allchannels_lock.lock();
  if (shuttingDownFlag) {
    channel->kill();
    allchannels_lock.unlock();
    callback->channelError(channel, ShuttingDownException());
  }
  channel->connect(callback);

  allchannels.insert(channel);
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "(create) All channels size: " << allchannels.size();
  }
  allchannels_lock.unlock();

  return channel;
}

DuplexChannelPtr ClientImpl::withChannel(const std::string& topic, const ChannelConnectCallbackPtr& callback) {
  HostAddress addr = topic2host[topic];
  DuplexChannelPtr channel = host2channel[addr];

  if (channel.get() == 0 || addr.isNullHost()) {
    ChannelHandlerPtr handler(new HedwigClientChannelHandler(shared_from_this()));
    channel = withNewChannel(topic, handler, callback);
    host2channel_lock.lock();
    host2channel[addr] = channel;
    host2channel_lock.unlock();
    return channel;
  } else {
    callback->channelConnected(channel);
  }

  return channel;
}

void ClientImpl::setHostForTopic(const std::string& topic, const HostAddress& host) {
  topic2host_lock.lock();
  topic2host[topic] = host;
  topic2host_lock.unlock();
}

bool ClientImpl::shuttingDown() const {
  return shuttingDownFlag;
}

/**
   A channel has just died. Remove it so we never give it to any other publisher or subscriber.
   
   This does not delete the channel. Some publishers or subscribers will still hold it and will be errored
   when they try to do anything with it. 
*/
void ClientImpl::channelDied(const DuplexChannelPtr& channel) {
  if (shuttingDownFlag) {
    return;
  }

  host2topics_lock.lock();
  host2channel_lock.lock();
  topic2host_lock.lock();
  allchannels_lock.lock();
  // get host
  HostAddress addr = channel->getHostAddress();
  
  for (Host2TopicsMap::iterator iter = host2topics.find(addr); iter != host2topics.end(); ++iter) {
    topic2host.erase((*iter).second);
  }
  host2topics.erase(addr);
  host2channel.erase(addr);

  allchannels.erase(channel); // channel should be deleted here

  allchannels_lock.unlock();
  host2topics_lock.unlock();
  host2channel_lock.unlock();
  topic2host_lock.unlock();
}

const Configuration& ClientImpl::getConfiguration() {
  return conf;
}

boost::asio::io_service& ClientImpl::getService() {
  return dispatcher.getService();
}
