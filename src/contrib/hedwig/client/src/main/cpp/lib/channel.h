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
#ifndef HEDWIG_CHANNEL_H
#define HEDWIG_CHANNEL_H

#include <hedwig/protocol.h>
#include <hedwig/callback.h>
#include <hedwig/client.h>
#include "util.h"
#include "data.h"
#include "eventdispatcher.h"

#include <tr1/memory>
#include <tr1/unordered_map>

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>

#include <boost/asio/ip/tcp.hpp>

namespace Hedwig {
  class ChannelException : public std::exception { };
  class UninitialisedChannelException : public ChannelException {};

  class ChannelConnectException : public ChannelException {};
  class CannotCreateSocketException : public ChannelConnectException {};
  class ChannelSetupException : public ChannelConnectException {};

  class ChannelDiedException : public ChannelException {};

  class ChannelWriteException : public ChannelException {};
  class ChannelReadException : public ChannelException {};
  class ChannelThreadException : public ChannelException {};

  class DuplexChannel;
  typedef boost::shared_ptr<DuplexChannel> DuplexChannelPtr;

  class ChannelConnectCallback {
  public:
    virtual void channelConnected(const DuplexChannelPtr& channel) = 0;
    virtual void channelError(const DuplexChannelPtr& channel, const std::exception& e) = 0;
  };
  typedef boost::shared_ptr<ChannelConnectCallback> ChannelConnectCallbackPtr;

  class ChannelHandler {
  public:
    virtual void messageReceived(const DuplexChannelPtr& channel, const PubSubResponsePtr& m) = 0;
    virtual void channelConnected(const DuplexChannelPtr& channel) = 0;

    virtual void channelDisconnected(const DuplexChannelPtr& channel, const std::exception& e) = 0;
    virtual void exceptionOccurred(const DuplexChannelPtr& channel, const std::exception& e) = 0;

    virtual ~ChannelHandler() {}
  };

  typedef boost::shared_ptr<ChannelHandler> ChannelHandlerPtr;


  class DuplexChannel : public boost::enable_shared_from_this<DuplexChannel> {
  public:
    DuplexChannel(EventDispatcher& dispatcher, const HostAddress& addr, const Configuration& cfg, const ChannelHandlerPtr& handler);
    static void connectCallbackHandler(DuplexChannelPtr channel, ChannelConnectCallbackPtr callback, 
			       const boost::system::error_code& error);
    void connect(const ChannelConnectCallbackPtr& callback);

    static void writeCallbackHandler(DuplexChannelPtr channel, OperationCallbackPtr callback, 
				     const boost::system::error_code& error, 
				     std::size_t bytes_transferred);
    void writeRequest(const PubSubRequest& m, const OperationCallbackPtr& callback);
    
    const HostAddress& getHostAddress() const;

    void storeTransaction(const PubSubDataPtr& data);
    PubSubDataPtr retrieveTransaction(long txnid);
    void failAllTransactions();

    static void sizeReadCallbackHandler(DuplexChannelPtr channel, const boost::system::error_code& error, 
					std::size_t bytes_transferred);
    static void messageReadCallbackHandler(DuplexChannelPtr channel, std::size_t messagesize, 
					   const boost::system::error_code& error, 
					   std::size_t bytes_transferred);
    static void readSize(DuplexChannelPtr channel);

    void startReceiving();
    bool isReceiving();
    void stopReceiving();
    
    void channelDisconnected(const std::exception& e);
    virtual void kill();

    ~DuplexChannel();
  private:
    EventDispatcher& dispatcher;

    HostAddress address;
    ChannelHandlerPtr handler;

    boost::asio::ip::tcp::socket socket;
    boost::asio::streambuf in_buf;
    boost::asio::streambuf out_buf;
    
    
    enum State { UNINITIALISED, CONNECTING, CONNECTED, DEAD };
    State state;
    
    typedef std::tr1::unordered_map<long, PubSubDataPtr> TransactionMap;

    /*    std::deque<ChannelConnectCallbackPtr> connectCallbacks;
	  Mutex connectCallbacks_lock;*/

    TransactionMap txnid2data;
    Mutex txnid2data_lock;
    Mutex destruction_lock;
  };
  

  struct DuplexChannelPtrHash : public std::unary_function<DuplexChannelPtr, size_t> {
    size_t operator()(const Hedwig::DuplexChannelPtr& channel) const {
      return reinterpret_cast<size_t>(channel.get());
    }
  };
};
#endif
