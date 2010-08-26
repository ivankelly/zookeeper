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
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <iostream>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <vector>
#include <utility>
#include <deque>
#include "channel.h"
#include "util.h"
#include "clientimpl.h"

#include <log4cpp/Category.hh>

static log4cpp::Category &LOG = log4cpp::Category::getInstance("hedwig."__FILE__);

const int MAX_MESSAGE_SIZE = 2*1024*1024; // 2 Meg

using namespace Hedwig;

namespace Hedwig {


}

DuplexChannel::DuplexChannel(EventDispatcher& dispatcher, const HostAddress& addr, 
			     const Configuration& cfg, const ChannelHandlerPtr& handler)
  : dispatcher(dispatcher), address(addr), handler(handler), 
    socket(dispatcher.getService()), state(UNINITIALISED), txnid2data_lock()
{
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "Creating DuplexChannel(" << this << ")";
  }
}

/*static*/ void DuplexChannel::connectCallbackHandler(DuplexChannelPtr channel, ChannelConnectCallbackPtr callback, 
						      const boost::system::error_code& error) {
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "DuplexChannel::connectCallbackHandler " << error;;
  }

  if (error) {
    callback->channelError(channel, ChannelConnectException());
    channel->state = DEAD;
    return;
  }

  channel->state = CONNECTED;

  boost::system::error_code ec;
  boost::asio::ip::tcp::no_delay option(true);

  channel->socket.set_option(option, ec);
  if (ec) {
    callback->channelError(channel, ChannelSetupException());
    channel->state = DEAD;
    return;
  } 
  callback->channelConnected(channel);

  channel->startReceiving();
}

void DuplexChannel::connect(const ChannelConnectCallbackPtr& callback) {  
  state = CONNECTING;

  boost::asio::ip::tcp::endpoint endp(boost::asio::ip::address_v4(address.ip()), address.port());
  boost::system::error_code error = boost::asio::error::host_not_found;

  socket.async_connect(endp, boost::bind(&DuplexChannel::connectCallbackHandler, 
					 shared_from_this(), callback,
					 boost::asio::placeholders::error)); 

  /*
  connectCallbacks_lock.lock();
  if (state != UNINITIALISED) {
    connectCallbacks.push_back(callback);
    return;
  }
  state = CONNECTING;
  connectCallbacks_lock.unlock();

  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "DuplexChannel(" << this << ")::connect " << address.getAddressString();
  }

  socketfd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
  
  if (-1 == socketfd) {
    LOG.errorStream() << "DuplexChannel(" << this << ") Unable to create socket";
    
    callback->channelError(shared_from_this(), CannotCreateSocketException());
  }

  int flag = 1;
  int res = 0;
  if ((res = setsockopt(socketfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int))) != 0){
    close(socketfd);
    LOG.errorStream() << "Error setting nodelay on (" << this << ") " << res;
    callback->channelError(shared_from_this(), ChannelSetupException());
  }

  reader = new ReadThread(shared_from_this(), socketfd, handler);
  writer = new WriteThread(shared_from_this(), socketfd, handler);


  connectCallbacks_lock.lock();
  connectCallbacks.push_back(callback);
  connectCallbacks_lock.unlock();
  
  handler->channelConnected(shared_from_this());
  reader->run();
  writer->run();*/
}

/*static*/ void DuplexChannel::messageReadCallbackHandler(DuplexChannelPtr channel, 
							  std::size_t message_size,
							  const boost::system::error_code& error, 
							  std::size_t bytes_transferred) {
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "DuplexChannel::messageReadCallbackHandler " << error << ", " << bytes_transferred;
  }

  if (error) {
    LOG.errorStream() << "Invalid read error (" << error << ") bytes_transferred (" << bytes_transferred << ")";
    channel->channelDisconnected(ChannelReadException());
    return;
  }

  std::istream is(&channel->in_buf);
  PubSubResponsePtr response(new PubSubResponse());
  bool err = response->ParseFromIstream(&is);

  if (!err) {
    LOG.errorStream() << "Error parsing message";

    channel->channelDisconnected(ChannelReadException());
    return;
  }

  if (channel->handler.get()) {
    channel->handler->messageReceived(channel, response);
  }

  DuplexChannel::readSize(channel);
}

/*static*/ void DuplexChannel::sizeReadCallbackHandler(DuplexChannelPtr channel, const boost::system::error_code& error, 
						       std::size_t bytes_transferred) {
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "DuplexChannel::sizeReadCallbackHandler " << error << ", " << bytes_transferred;
  }

  if (error) {
    LOG.errorStream() << "Invalid read error (" << error << ") bytes_transferred (" << bytes_transferred << ")";
    channel->channelDisconnected(ChannelReadException());
    return;
  }
  
  if (channel->in_buf.size() < sizeof(uint32_t)) {
    LOG.errorStream() << "Not enough data in stream. Must have been an error reading. Closing channel";
    channel->channelDisconnected(ChannelReadException());
    return;
  }

  uint32_t size;
  std::istream is(&channel->in_buf);
  is.read((char*)&size, sizeof(uint32_t));
  size = ntohl(size);

  int toread = size - channel->in_buf.size();

  if (toread < 0) {
    DuplexChannel::messageReadCallbackHandler(channel, size, error, 0);
  } else {
    boost::asio::async_read(channel->socket, channel->in_buf,
			    boost::asio::transfer_at_least(toread),
			    boost::bind(&DuplexChannel::messageReadCallbackHandler, channel, size,
					boost::asio::placeholders::error, 
					boost::asio::placeholders::bytes_transferred));
  }
}

/*static*/ void DuplexChannel::readSize(DuplexChannelPtr channel) {
  int toread = sizeof(uint32_t) - channel->in_buf.size();

  if (toread < 0) {
    DuplexChannel::sizeReadCallbackHandler(channel, boost::system::error_code(), 0);
  } else {
    //  in_buf_size.prepare(sizeof(uint32_t));
    boost::asio::async_read(channel->socket, channel->in_buf, 
			    boost::asio::transfer_at_least(sizeof(uint32_t)),
			    boost::bind(&DuplexChannel::sizeReadCallbackHandler, 
					channel, 
					boost::asio::placeholders::error, 
					boost::asio::placeholders::bytes_transferred));
  }
}

void DuplexChannel::startReceiving() {
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "DuplexChannel::startReceiving";
  }
  
  DuplexChannel::readSize(shared_from_this());
}

bool DuplexChannel::isReceiving() {
  return true;
}

void DuplexChannel::stopReceiving() {
  
}

const HostAddress& DuplexChannel::getHostAddress() const {
  return address;
}

void DuplexChannel::channelDisconnected(const std::exception& e) {
  state = DEAD;
  if (handler.get()) {
    handler->channelDisconnected(shared_from_this(), e);
  }
}

void DuplexChannel::kill() {
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "Killing duplex channel (" << this << ")";
  }    

  destruction_lock.lock();
  if (state == CONNECTING || state == CONNECTED) {
    state = DEAD;
    
    destruction_lock.unlock();

    socket.cancel();
    socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both);
    socket.close();
  } else {
    destruction_lock.unlock();
  }
  handler = ChannelHandlerPtr(); // clear the handler in case it ever referenced the channel*/
}

DuplexChannel::~DuplexChannel() {
  /** If we are going away, fail all transactions that haven't been completed */
  failAllTransactions();
  kill();


  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "Destroying DuplexChannel(" << this << ")";
  }
}

/*static*/ void DuplexChannel::writeCallbackHandler(DuplexChannelPtr channel, OperationCallbackPtr callback,
						    const boost::system::error_code& error, 
						    std::size_t bytes_transferred) {
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "DuplexChannel::writeCallbackHandler " << error << ", " << bytes_transferred;
  }

  if (error) {
    callback->operationFailed(ChannelWriteException());
    channel->channelDisconnected(ChannelWriteException());
    return;
  }
  channel->out_buf.consume(bytes_transferred);
  callback->operationComplete();
}

void DuplexChannel::writeRequest(const PubSubRequest& m, const OperationCallbackPtr& callback) {
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "DuplexChannel::writeRequest ";
  }

  if (state != CONNECTED && state != CONNECTING) {
    LOG.errorStream() << "Tried to write transaction [" << m.txnid() << "] to a channel [" << this << "] which is " << (state == DEAD ? "DEAD" : "UNINITIALISED");
    callback->operationFailed(UninitialisedChannelException());
  }

  std::ostream os(&out_buf);
  uint32_t size = htonl(m.ByteSize());
  os.write((char*)&size, sizeof(uint32_t));
  
  bool err = m.SerializeToOstream(&os);
  if (!err) {
    callback->operationFailed(ChannelWriteException());
    channelDisconnected(ChannelWriteException());
    return;
  }

  boost::asio::async_write(socket, out_buf, 
			   boost::bind(&DuplexChannel::writeCallbackHandler, 
				       shared_from_this(), 
				       callback,
				       boost::asio::placeholders::error, 
				       boost::asio::placeholders::bytes_transferred));
}

/**
   Store the transaction data for a request.
*/
void DuplexChannel::storeTransaction(const PubSubDataPtr& data) {
  txnid2data_lock.lock();
  txnid2data[data->getTxnId()] = data;
  txnid2data_lock.unlock();;
}

/**
   Give the transaction back to the caller. 
*/
PubSubDataPtr DuplexChannel::retrieveTransaction(long txnid) {
  txnid2data_lock.lock();
  PubSubDataPtr data = txnid2data[txnid];
  txnid2data.erase(txnid);
  txnid2data_lock.unlock();
  return data;
}

void DuplexChannel::failAllTransactions() {
  txnid2data_lock.lock();
  for (TransactionMap::iterator iter = txnid2data.begin(); iter != txnid2data.end(); ++iter) {
    PubSubDataPtr& data = (*iter).second;
    data->getCallback()->operationFailed(ChannelDiedException());
  }
  txnid2data.clear();
  txnid2data_lock.unlock();
}

/** 
Entry point for pthread initialisation
*/
/*void* ThreadEntryPoint(void *obj) {
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "Thread entered (" << obj << ")";
  }

  RunnableThread* thread = (RunnableThread*) obj;
  thread->entryPoint();
}
 
RunnableThread::RunnableThread(const DuplexChannelPtr& channel, const ChannelHandlerPtr& handler) 
  : channel(channel), handler(handler)
{
  //  pthread_cond_init(&deathlock, NULL);
}

void RunnableThread::run() {
  int ret;

  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "Running thread (" << this << ")";
  }    
  
  pthread_attr_init(&attr);
  ret = pthread_create(&thread, &attr, ThreadEntryPoint, this);
  if (ret != 0) {
    LOG.errorStream() << "Error creating thread (" << this << "). Notifying handler.";
    handler->exceptionOccurred(channel, ChannelThreadException());
  }
}

void RunnableThread::kill() {
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "Killing thread (" << this << ")";
  }    

  pthread_cancel(thread);
  pthread_join(thread, NULL);

  pthread_attr_destroy(&attr);
}

RunnableThread::~RunnableThread() {
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "Deleting thread (" << this << ")";
  }    
  }*/
/**
Writer thread
*//*
WriteThread::WriteThread(const DuplexChannelPtr& channel, int socketfd, const ChannelHandlerPtr& handler) 
  : RunnableThread(channel, handler), socketfd(socketfd), packetsAvailableWaitCondition(requestQueue), queueMutex(), dead(false) {
  
}

// should probably be using a queue here.
void WriteThread::writeRequest(const PubSubRequest& m, const OperationCallbackPtr& callback) {
  if (LOG.isDebugEnabled()) {
    LOG.debugStream() << "Adding message to queue " << &m;
  }
  packetsAvailableWaitCondition.lock();
  queueMutex.lock();
  requestQueue.push_back(RequestPair(&m, callback));
  queueMutex.unlock();;

  packetsAvailableWaitCondition.signalAndUnlock();
}
  
void WriteThread::entryPoint() {
  if (-1 == ::connect(socketfd, (const struct sockaddr *)&(channel->getHostAddress().socketAddress()), sizeof(struct sockaddr_in))) {
    LOG.errorStream() << "DuplexChannel(" << this << ") Could not connect socket";
    close(socketfd);
    
    channel->channelDisconnected(CannotConnectException());
  }
  channel->channelConnected();

  while (true) {
    packetsAvailableWaitCondition.wait();

    if (dead) {
      if (LOG.isDebugEnabled()) {
	LOG.debugStream() << "returning from thread " << this;
      }
      return;
    }
    while (!requestQueue.empty()) { 
      queueMutex.lock();;
      RequestPair currentRequest = requestQueue.front();;
      requestQueue.pop_front();
      queueMutex.unlock();
      if (LOG.isDebugEnabled()) {
	LOG.debugStream() << "Writing message to socket " << currentRequest.first;
      }
      
      uint32_t size = htonl(currentRequest.first->ByteSize());
      write(socketfd, &size, sizeof(size));
      
      bool res = currentRequest.first->SerializeToFileDescriptor(socketfd);
      
      if (!res || errno != 0) {
	LOG.errorStream() << "Error writing to socket (" << this << ") errno(" << errno << ") res(" << res << "). Disconnected.";
	ChannelWriteException e;
	
	currentRequest.second->operationFailed(e);
	channel->kill(); // make sure it's dead
	handler->channelDisconnected(channel, e);
	
	return;
      } else {
	currentRequest.second->operationComplete();
      }
    }  
  }
}

void WriteThread::kill() {
  dead = true;
  packetsAvailableWaitCondition.lock();
  packetsAvailableWaitCondition.kill();
  packetsAvailableWaitCondition.signalAndUnlock();
  
  RunnableThread::kill();
}

WriteThread::~WriteThread() {
  queueMutex.unlock();
}
  */
/**
Reader Thread
*/
/*
ReadThread::ReadThread(const DuplexChannelPtr& channel, int socketfd, const ChannelHandlerPtr& handler) 
  : RunnableThread(channel, handler), socketfd(socketfd) {
}
  
void ReadThread::entryPoint() {
  PubSubResponse* response = new PubSubResponse();
  uint8_t* dataarray = NULL;//(uint8_t*)malloc(MAX_MESSAGE_SIZE); // shouldn't be allocating every time. check that there's a max size
  int currentbufsize = 0;

  struct pollfd pfd = { socketfd, POLLRDNORM };
  int ret = poll(&pfd, 1, -1);
  
  if (ret < 0 || (pfd.revents & POLLERR) || (pfd.revents & POLLHUP)) {
    LOG.errorStream() << "Socket never became readable, this thread is going away ret(" << ret << ") revents(" << pfd.revents << ")";
    return;
  }

  while (true) {
    uint32_t size = 0;
    int bytesread = 0;

    bytesread = read(socketfd, &size, sizeof(size));
    size = ntohl(size);
    if (LOG.isDebugEnabled()) {
      LOG.debugStream() << "Start reading packet of size: " << size;
    }
    if (bytesread < 1 || size > MAX_MESSAGE_SIZE) {
      LOG.errorStream() << "Zero read from socket or unreasonable size read, size(" << size << ") errno(" << errno << ") " << strerror(errno);
      channel->kill(); // make sure it's dead
      handler->channelDisconnected(channel, ChannelReadException());
      break;
    }

    if (currentbufsize < size) {
      dataarray = (uint8_t*)realloc(dataarray, size);
    }
    if (dataarray == NULL) {
      LOG.errorStream() << "Error allocating input buffer of size " << size << " errno(" << errno << ") " << strerror(errno);
      channel->kill(); // make sure it's dead
      handler->channelDisconnected(channel, ChannelReadException());
      
      break;
    }
    
    memset(dataarray, 0, size);
    bytesread = read(socketfd, dataarray, size);
    bool res = response->ParseFromArray(dataarray, size);


    if (LOG.isDebugEnabled()) {
      LOG.debugStream() << "Packet read ";
    }
    
    if (!res && errno != 0 || bytesread < size) {
      LOG.errorStream() << "Error reading from socket (" << this << ") errno(" << errno << ") res(" << res << "). Disconnected.";
      channel->kill(); // make sure it's dead
      handler->channelDisconnected(channel, ChannelReadException());

      break;
    } else {
      handler->messageReceived(channel, *response);
    }
  }
  free(dataarray);
  delete response;
}

ReadThread::~ReadThread() {
}
*/
