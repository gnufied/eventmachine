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

package com.rubyeventmachine;

import java.io.*;
import java.nio.channels.*;
import java.util.*;
import java.nio.*;
import java.net.*;
import java.util.concurrent.atomic.*;
import java.security.*;

public class EmReactor {

	
	public final int EM_TIMER_FIRED = 100;
	public final int EM_CONNECTION_READ = 101;
	public final int EM_CONNECTION_UNBOUND = 102;
	public final int EM_CONNECTION_ACCEPTED = 103;
	public final int EM_CONNECTION_COMPLETED = 104;
	public final int EM_LOOPBREAK_SIGNAL = 105;
	
	private Selector mySelector;
	private TreeMap<Long, String> Timers;
	private TreeMap<String, EventableChannel> Connections;
	private TreeMap<String, ServerSocketChannel> Acceptors;
	private TreeMap<Long, Set<String>> interestedInInactivity;
	
	private boolean bRunReactor;
	private long BindingIndex;
	private ByteBuffer EmptyByteBuffer;
	private AtomicBoolean loopBreaker;
	private ByteBuffer myReadBuffer;
	private int timerQuantum;
	
	public EmReactor() {
		Timers = new TreeMap<Long, String>();
		Connections = new TreeMap<String, EventableChannel>();
		Acceptors = new TreeMap<String, ServerSocketChannel>();
		interestedInInactivity = new TreeMap<Long, Set<String>>();
		
		BindingIndex = 100000;
		EmptyByteBuffer = ByteBuffer.allocate(0);
		loopBreaker = new AtomicBoolean();
		loopBreaker.set(false);
		myReadBuffer = ByteBuffer.allocate(32*1024); // don't use a direct buffer. Ruby doesn't seem to like them.
		timerQuantum = 98;
	}
		
 	/**
 	 * This is a no-op stub, intended to be overridden in user code.
 	 * @param sig
 	 * @param eventType
 	 * @param data
 	 */
	public void eventCallback (String sig, int eventType, ByteBuffer data) {
		System.out.println ("Default callback: "+sig+" "+eventType+" "+data);
		//stringEventCallback (sig, eventType, new String (data.array(), data.position(), data.limit()));
		
	}
	
	public void run() throws IOException {
 		mySelector = Selector.open();
 		bRunReactor = true;
 		
 		//int n = 0;
 		for (;;) {
 			//System.out.println ("loop "+ (n++));
 			if (!bRunReactor) break;
 			runLoopbreaks();
 			if (!bRunReactor) break;
 			runTimers();
 			if (!bRunReactor) break;
 			checkForInactivity();
 			if (!bRunReactor) break;
 			
 			mySelector.select(timerQuantum);
 			 	
 			Iterator<SelectionKey> it = mySelector.selectedKeys().iterator();
 			while (it.hasNext()) {
 				SelectionKey k = it.next();
 				it.remove();

 				try {
 					if(k.isAcceptable()) acceptClient(k);
 					else if(k.isReadable()) readData(k);
 					else if(k.isWritable()) handleWrite(k);
 					else if(k.isConnectable()) completeExternalConnection(k);
 				} 				
 				catch (CancelledKeyException e) {
 					e.printStackTrace();
 				}
 				catch (IOException ex){
 					ex.printStackTrace();
 				} 				
  			}
   		}
 		
 		close();
	}
	
	public void handleWrite(SelectionKey k) throws IOException {
		EventableChannel ec = (EventableChannel) k.attachment();
		ec.updateActivityTimeStamp();
		try {
			ec.writeOutboundData();
		} catch (ClientDisconnectException ex) {
			cleanupConnection(ec);
		}
	}

	public void readData(SelectionKey k) throws IOException {
		EventableChannel ec = (EventableChannel) k.attachment();
		myReadBuffer.clear();
		try {
			ec.readInboundData(myReadBuffer);
			myReadBuffer.flip();
			ec.updateActivityTimeStamp();
			eventCallback(ec.getBinding(), EM_CONNECTION_READ, myReadBuffer);
		} catch (ClientDisconnectException e) {
			System.out.println("There was an exception here");
			cleanupConnection(ec);
		}
	}

	public void acceptClient(SelectionKey k) throws IOException {
		ServerSocketChannel ss = (ServerSocketChannel) k.channel();
		SocketChannel sn;
		while ((sn = ss.accept()) != null) {
			sn.configureBlocking(false);
			String b = createBinding();
			EventableSocketChannel ec = new EventableSocketChannel(sn, b,
					mySelector, SelectionKey.OP_READ);
			Connections.put(b, ec);
			eventCallback((String) k.attachment(), EM_CONNECTION_ACCEPTED,
					ByteBuffer.wrap(b.getBytes()));
		}
	}

	public void completeExternalConnection(SelectionKey k) throws IOException {
		EventableSocketChannel ec = (EventableSocketChannel) k.attachment();
		if (ec.finishConnecting()) {
			eventCallback(ec.getBinding(), EM_CONNECTION_COMPLETED,
					EmptyByteBuffer);
		} else {
			Connections.remove(ec.getBinding());
			k.channel().close();
			eventCallback(ec.getBinding(), EM_CONNECTION_UNBOUND,
					EmptyByteBuffer);
		}
	}

	public void cleanupConnection(EventableChannel ec) {
		eventCallback(ec.getBinding(), EM_CONNECTION_UNBOUND, EmptyByteBuffer);
		Connections.remove(ec.getBinding());
		long inactivitykey = ec.currentInActivityKey();
		Set<String> toRemove = interestedInInactivity.get(inactivitykey);
		if (toRemove != null) {
			toRemove.remove(ec.getBinding());
			interestedInInactivity.put(inactivitykey, toRemove);
		}
	}

	public void checkForInactivity() {
		long now = System.currentTimeMillis();
		Set<String> toAddback = new HashSet<String>();

		while (!interestedInInactivity.isEmpty()) {
			long k = interestedInInactivity.firstKey();
			if (k > now) {
				break;
			}
			Set<String> bindingStringSet = interestedInInactivity.remove(k);
			Iterator<String> iterator = bindingStringSet.iterator();

			while (iterator.hasNext()) {
				String key = iterator.next();
				EventableChannel t = Connections.get(key);
				if (t != null) {
					if (t.isInactive()){
						System.out.println("Schedule a close on the connection");
						t.scheduleClose(true);	
					}
					else
						toAddback.add(t.getBinding());
				}
			}
		}
		if (!toAddback.isEmpty())
			addToInactivityCheck(toAddback);
	}

	public void addToInactivityCheck(Set<String> sids) {
		Iterator<String> iterator = sids.iterator();
		while (iterator.hasNext()) {
			String sid = iterator.next();
			EventableChannel t = Connections.get(sid);
			setCommInactivityTimeout(t);
		}
	}

	
	void close() throws IOException {
		mySelector.close();
		mySelector = null;

		// run down open connections and sockets.
		Iterator<ServerSocketChannel> i = Acceptors.values().iterator();
		while (i.hasNext()) {
			i.next().close();
		}
		
		Iterator<EventableChannel> i2 = Connections.values().iterator();
		while (i2.hasNext())
			i2.next().close();
	}
	
	void runLoopbreaks() {
		if (loopBreaker.getAndSet(false)) {
			eventCallback ("", EM_LOOPBREAK_SIGNAL, EmptyByteBuffer);
		}
	}
	
	public void stop() {
		bRunReactor = false;
		signalLoopbreak();
	}
	
	void runTimers() {
		long now = new Date().getTime();
		while (!Timers.isEmpty()) {
			long k = Timers.firstKey();
			//System.out.println (k - now);
			if (k > now)
				break;
			String s = Timers.remove(k);
			eventCallback ("", EM_TIMER_FIRED, ByteBuffer.wrap(s.getBytes()));
		}
	}
	
	public String installOneshotTimer (int milliseconds) {
		BindingIndex++;
		String s = createBinding();
		Timers.put(new Date().getTime() + milliseconds, s);
		return s;
	}
	
	public String startTcpServer (SocketAddress sa) throws EmReactorException {
		try {
			ServerSocketChannel server = ServerSocketChannel.open();
			server.configureBlocking(false);
			server.socket().bind (sa);
			String s = createBinding();
			Acceptors.put(s, server);
			server.register(mySelector, SelectionKey.OP_ACCEPT, s);
			return s;
		} catch (IOException e) {
			// TODO, should parameterize this exception better.
			throw new EmReactorException ("unable to open socket acceptor");
		}
	}
	
	public String startTcpServer (String address, int port) throws EmReactorException {
		return startTcpServer (new InetSocketAddress (address, port));
		/*
		ServerSocketChannel server = ServerSocketChannel.open();
		server.configureBlocking(false);
		server.socket().bind(new java.net.InetSocketAddress(address, port));
		String s = createBinding();
		Acceptors.put(s, server);
		server.register(mySelector, SelectionKey.OP_ACCEPT, s);
		return s;
		*/
	}

	public void stopTcpServer (String signature) throws IOException {
		ServerSocketChannel server = Acceptors.remove(signature);
		if (server != null)
			server.close();
		else
			throw new RuntimeException ("failed to close unknown acceptor");
	}
	

	public String openUdpSocket (String address, int port) throws IOException {
		return openUdpSocket (new InetSocketAddress (address, port));
	}
	/**
	 * 
	 * @param address
	 * @param port
	 * @return
	 * @throws IOException
	 */
	public String openUdpSocket (InetSocketAddress address) throws IOException {
		// TODO, don't throw an exception out of here.
		DatagramChannel dg = DatagramChannel.open();
		dg.configureBlocking(false);
		dg.socket().bind(address);
		String b = createBinding();
		EventableChannel ec = new EventableDatagramChannel (dg, b, mySelector);
		dg.register(mySelector, SelectionKey.OP_READ, ec);
		Connections.put(b, ec);
		return b;
	}
	
	public void sendData (String sig, ByteBuffer bb) throws IOException {
		try {
			EventableChannel ec = Connections.get(sig);
			if(ec != null) ec.scheduleOutboundData(bb);
		} catch(ClientDisconnectException ex){
			System.out.println("Error while writing data to socket");
			cleanupConnection(Connections.get(sig));
		}
	}
	
	public void sendData (String sig, byte[] data) throws IOException {
		sendData (sig, ByteBuffer.wrap(data));
		//(Connections.get(sig)).scheduleOutboundData( ByteBuffer.wrap(data.getBytes()));
	}
	public void setCommInactivityTimeout(EventableChannel t) {
		long timeout = t.getLastActivity() + t.getInActivityPeriod() * 1000;
		Set<String> oldSet = interestedInInactivity.get(timeout);
		String sig = t.getBinding();

		if (oldSet == null) {
			oldSet = new HashSet<String>();
			oldSet.add(sig);
		} else
			oldSet.add(sig);

		interestedInInactivity.put(timeout, oldSet);
		t.setInactivityKey(timeout);
	}

	public void setCommInactivityTimeout(String sig, int seconds) {
		long timeout = System.currentTimeMillis() + seconds * 1000;
		Set<String> oldSet = interestedInInactivity.get(timeout);

		if (oldSet == null) {
			oldSet = new HashSet<String>();
			oldSet.add(sig);
		} else {
			oldSet.add(sig);
		}
		interestedInInactivity.put(timeout, oldSet);
		EventableChannel conn = Connections.get(sig);
		conn.setCommInactivityTimeout(seconds);
		conn.setInactivityKey(timeout);
	}
	
	/**
	 * 
	 * @param sig
	 * @param data
	 * @param length
	 * @param recipAddress
	 * @param recipPort
	 */
	public void sendDatagram (String sig, String data, int length, String recipAddress, int recipPort) {
		sendDatagram (sig, ByteBuffer.wrap(data.getBytes()), recipAddress, recipPort);
	}
	
	/**
	 * 
	 * @param sig
	 * @param bb
	 * @param recipAddress
	 * @param recipPort
	 */
	public void sendDatagram (String sig, ByteBuffer bb, String recipAddress, int recipPort) {
		(Connections.get(sig)).scheduleOutboundDatagram( bb, recipAddress, recipPort);
	}

	/**
	 * 
	 * @param address
	 * @param port
	 * @return
	 * @throws ClosedChannelException
	 */
	public String connectTcpServer (String address, int port) throws ClosedChannelException {
		return connectTcpServer(null, 0, address, port);
	}

	/**
	 *
	 * @param bindAddr
	 * @param bindPort
	 * @param address
	 * @param port
	 * @return
	 * @throws ClosedChannelException
	 */
	public String connectTcpServer (String bindAddr, int bindPort, String address, int port) throws ClosedChannelException {
		String b = createBinding();
				
		try {
			SocketChannel sc = SocketChannel.open();
			sc.configureBlocking(false);
			if (bindAddr != null)
				sc.socket().bind(new InetSocketAddress (bindAddr, bindPort));

			EventableSocketChannel ec = new EventableSocketChannel (sc, b, mySelector,0);

			if (sc.connect (new InetSocketAddress (address, port))) {
				// Connection returned immediately. Can happen with localhost connections.
				// WARNING, this code is untested due to lack of available test conditions.
				// Ought to be be able to come here from a localhost connection, but that
				// doesn't happen on Linux. (Maybe on FreeBSD?)
				// The reason for not handling this until we can test it is that we
				// really need to return from this function WITHOUT triggering any EM events.
				// That's because until the user code has seen the signature we generated here,
				// it won't be able to properly dispatch them. The C++ EM deals with this
				// by setting pending mode as a flag in ALL eventable descriptors and making
				// the descriptor select for writable. Then, it can send UNBOUND and
				// CONNECTION_COMPLETED on the next pass through the loop, because writable will
				// fire.
				throw new RuntimeException ("immediate-connect unimplemented");
 			}
			else {
				Connections.put (b, ec);
				ec.setConnectPending();
			}
		} catch (IOException e) {
			// Can theoretically come here if a connect failure can be determined immediately.
			// I don't know how to make that happen for testing purposes.
			throw new RuntimeException ("immediate-connect unimplemented"); 
		}
		return b;
	}

	public void closeConnection (String sig, boolean afterWriting) throws ClosedChannelException {
		Connections.get(sig).scheduleClose (afterWriting);
	}
	
	String createBinding() {
		return new String ("BND_" + (++BindingIndex));
	}
	
	public void signalLoopbreak() {
		loopBreaker.set(true);
		mySelector.wakeup();
	}
	
	public void startTls (String sig) throws NoSuchAlgorithmException, KeyManagementException {
		Connections.get(sig).startTls();
	}
	
	public void setTimerQuantum (int mills) {
		if (mills < 5 || mills > 2500)
			throw new RuntimeException ("attempt to set invalid timer-quantum value: "+mills);
		timerQuantum = mills;
	}
}
