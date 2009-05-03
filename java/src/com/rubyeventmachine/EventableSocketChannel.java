/**
 * $Id$
 * 
 * Author:: Francis Cianfrocca (gmail: blackhedd)
 * Homepage::  http://rubyeventmachine.com
 * Date:: 15 Jul 2007
 * 
 * See EventMachine and EventMachine::Connection for documentation and
 * usage examples.
 * 
 *
 *----------------------------------------------------------------------------
 *
 * Copyright (C) 2006-07 by Francis Cianfrocca. All Rights Reserved.
 * Gmail: blackhedd
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of either: 1) the GNU General Public License
 * as published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version; or 2) Ruby's License.
 * 
 * See the file COPYING for complete licensing information.
 *
 *---------------------------------------------------------------------------
 *
 * 
 */

/**
 * 
 */
package com.rubyeventmachine;

/**
 * @author francis
 *
 */

import java.nio.channels.*;
import java.nio.*;
import java.util.*;
import java.io.*;
import javax.net.ssl.*;
import javax.net.ssl.SSLEngineResult.*;

import java.security.*;

public class EventableSocketChannel implements EventableChannel {
	
	// TODO, must refactor this to permit channels that aren't sockets.
	SocketChannel channel;
	String binding;
	Selector selector;
	ArrayDeque<ByteBuffer> outboundQ;
	boolean bCloseScheduled;
	boolean connectionStatus = false;

	long inactivityPeriod;
	long lastActivity;
	long inActivityKey;
	boolean writeScheduled;

	boolean bConnectPending;
	
	SSLEngine sslEngine;
	SSLContext sslContext;
	
	SelectionKey channelKey;
	public EventableSocketChannel(SocketChannel sc, String _binding,
			Selector sel, int ops) throws ClosedChannelException {
		writeScheduled = false;
		channel = sc;
		binding = _binding;
		selector = sel;
		bCloseScheduled = false;
		bConnectPending = false;
		outboundQ = new ArrayDeque<ByteBuffer>();
		lastActivity = new Date().getTime();
		if (ops != 0) {
			connectionStatus = true;
			channelKey = sc.register(selector, ops, this);
		}
	}
	
	public String getBinding() {
		return binding;
	}
	
	/**
	 * Terminate with extreme prejudice. Don't assume there will be another pass through
	 * the reactor core.
	 */
	public void close() {
		try {
			channel.close();
		} catch (IOException e) {
		}
	}
	
	public void scheduleOutboundData(ByteBuffer bb) { 
		try { 
		    if(sslEngine != null) { 
			ByteBuffer b= ByteBuffer.allocate(32*1024);
			sslEngine.wrap(bb,b);
			b.flip();
			outboundQ.offer(b);
		    } else outboundQ.offer(bb);
		} catch(SSLException e) { 
		    throw new RuntimeException("Error while writing to ssl channel");
		}
		writeOutboundData();
	}

	public void scheduleOutboundDatagram (ByteBuffer bb, String recipAddress, int recipPort) {
		throw new RuntimeException ("datagram sends not supported on this channel");
	}
	
	/**
	 * Called by the reactor when we have selected readable.
	 */
	public void readInboundData(ByteBuffer bb){
		int dataLength = 0;
		try {
			dataLength = channel.read(bb);
		} catch (IOException e) {
			System.out.println("I am in readInboundData");
			signalConnectionFail();
		}

		if (dataLength == -1){
			System.out.println("I am throwing from data length");
			signalConnectionFail();
		}
	}
	
	public void signalConnectionFail() {
		System.out.println("Cancel the key from taking part in polling");
		connectionStatus = false;
		channelKey.cancel();
		try {
			channel.close();
		} catch(IOException ex) {}
		
		throw new ClientDisconnectException("Client closed the connection");
	}
	
	/**
	 * Called by the reactor when we have selected writable.
	 * Return false to indicate an error that should cause the connection to close.
	 * We can get here with an empty outbound buffer if bCloseScheduled is true.
	 * TODO, VERY IMPORTANT: we're here because we selected writable, but it's always
	 * possible to become unwritable between the poll and when we get here. The way
	 * this code is written, we're depending on a nonblocking write NOT TO CONSUME
	 * the whole outbound buffer in this case, rather than firing an exception.
	 * We should somehow verify that this is indeed Java's defined behavior.
	 * Also TODO, see if we can use gather I/O rather than one write at a time.
	 * Ought to be a big performance enhancer.
	 * @return
	 */
	public boolean writeOutboundData(){
		while (!outboundQ.isEmpty()) {
			ByteBuffer b = outboundQ.peek();
			int n = 0;
			try {
				if (b.hasRemaining())
					n = channel.write(b);
			} catch (IOException e) {
				signalConnectionFail();
				//return false;
			}
			// Did we consume the whole outbound buffer? If yes,
			// pop it off and keep looping. If no, the outbound network
			// buffers are full, so break out of here.
			if (!b.hasRemaining()) outboundQ.poll();
			else break;
		}

		if (outboundQ.isEmpty()){
			cancelWrite();	
		}
		else if (!outboundQ.isEmpty() && !writeScheduled) {
			writeScheduled = true;
			channelKey.interestOps(SelectionKey.OP_WRITE);
		}

		// ALWAYS drain the outbound queue before triggering a connection close.
		// If anyone wants to close immediately, they're responsible for
		// clearing
		// the outbound queue.
		if(bCloseScheduled && outboundQ.isEmpty()) {
			signalConnectionFail();
			return false;
		} else return true;
	}
	
	public void cancelWrite() {
		if (writeScheduled) {
			writeScheduled = false;
			channelKey.interestOps(SelectionKey.OP_READ);
		}
	}

	public void setConnectPending() throws ClosedChannelException {
		channel.register(selector, SelectionKey.OP_CONNECT, this);
		bConnectPending = true;
	}
	
	/**
	 * Called by the reactor when we have selected connectable.
	 * Return false to indicate an error that should cause the connection to close.
	 * @throws ClosedChannelException
	 */
	public boolean finishConnecting() throws ClosedChannelException {
		try {
			channel.finishConnect();
		}
		catch (IOException e) {
			return false;
		}
		bConnectPending = false;
		channelKey = channel.register(selector, SelectionKey.OP_READ, this);
		return true;
	}
	
	public void scheduleClose (boolean afterWriting) {
		// TODO: What the hell happens here if bConnectPending is set?
		if (!afterWriting) outboundQ.clear();
		else {
			writeScheduled = true;
			channelKey.interestOps(SelectionKey.OP_WRITE);
		}		
		bCloseScheduled = true;
	}
	
	public void startTls() {
		if (sslEngine == null) {
			try {
				sslContext = SSLContext.getInstance("TLS");
				sslContext.init(null, null, null); // TODO, fill in the parameters.
				sslEngine = sslContext.createSSLEngine(); // TODO, should use the parameterized version, to get Kerb stuff and session re-use.
				sslEngine.setUseClientMode(false);
			} catch (NoSuchAlgorithmException e) {
				throw new RuntimeException ("unable to start TLS"); // TODO, get rid of this.				
			} catch (KeyManagementException e) {
				throw new RuntimeException ("unable to start TLS"); // TODO, get rid of this.				
			}
		}
		System.out.println ("Starting TLS");
	}
	
	public ByteBuffer dispatchInboundData (ByteBuffer bb) throws SSLException {
		if (sslEngine != null) {
			if (true) throw new RuntimeException ("TLS currently unimplemented");
			System.setProperty("javax.net.debug", "all");
			ByteBuffer w = ByteBuffer.allocate(32*1024); // TODO, WRONG, preallocate this buffer.
			SSLEngineResult res = sslEngine.unwrap(bb, w);
			if (res.getHandshakeStatus() == HandshakeStatus.NEED_TASK) {
				Runnable r;
				while ((r = sslEngine.getDelegatedTask()) != null) {
					r.run();
				}
			}
			System.out.println (bb);
			w.flip();
			return w;
		}
		else
			return bb;
	}
	
	public void setCommInactivityTimeout(long seconds) {
		inactivityPeriod = seconds;
	}

	public boolean isInactive() {
		long now = new Date().getTime();
		if ((now - lastActivity) / 1000 > inactivityPeriod) {
			return true;
		} else {
			return false;
		}
	}

	public void updateActivityTimeStamp() {
		lastActivity = new Date().getTime();
	}

	public long getLastActivity() {
		return lastActivity;
	}

	public long getInActivityPeriod() {
		return inactivityPeriod;
	}

	public void setInactivityKey(long mills) {
		inActivityKey = mills;
	}

	public long currentInActivityKey() {
		return inActivityKey;
	}

	public SocketChannel getChannel() {
		return channel;
	}
}
