package com.periscope.consensus;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class RaftTransport implements AutoCloseable {

	private final int port;
	private final ObjectMapper mapper;
	private final AtomicBoolean running = new AtomicBoolean(false);
	private ServerSocket serverSocket;

	public RaftTransport(int port) {
		this.port = port;
		this.mapper = new ObjectMapper();
		// Prevent Jackson from closing the socket streams after reading/writing!
		this.mapper.disable(com.fasterxml.jackson.core.JsonParser.Feature.AUTO_CLOSE_SOURCE);
		this.mapper.disable(com.fasterxml.jackson.core.JsonGenerator.Feature.AUTO_CLOSE_TARGET);
	}

	/**
	 * Starts the TCP server listening for incoming Raft RPCs.
	 * @param handler A function that takes an incoming RaftMessage and returns a reply RaftMessage
	 */
	public void startListening(Function<RaftMessage, RaftMessage> handler)
		throws Exception {
		this.serverSocket = new ServerSocket(port);
		this.running.set(true);

		Thread.ofVirtual().start(() -> {
			while (running.get()) {
				Socket clientSocket;
				try {
					clientSocket = serverSocket.accept();
				} catch (IOException e) {
					if (running.get()) {
						e.printStackTrace();
					}
					continue;
				}
				Thread.ofVirtual().start(() ->
					handleClient(clientSocket, handler)
				);
			}
		});
	}

	private void handleClient(
		Socket socket,
		Function<RaftMessage, RaftMessage> handler
	) {
		try (socket) {
			RaftMessage request = mapper.readValue(
				socket.getInputStream(),
				RaftMessage.class
			);
			RaftMessage response = handler.apply(request);
			mapper.writeValue(socket.getOutputStream(), response);
		} catch (Exception e) {
			if (running.get()) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Sends a RequestVote RPC to a peer and waits for the response.
	 */
	public RaftMessage.RequestVoteResponse sendRequestVote(
		String peerHostPort,
		RaftMessage.RequestVote request
	) throws Exception {
		return (RaftMessage.RequestVoteResponse) sendRpc(peerHostPort, request);
	}

	/**
	 * Sends an AppendEntries (Heartbeat) RPC to a peer and waits for the response.
	 */
	public RaftMessage.AppendEntriesResponse sendHeartbeat(
		String peerHostPort,
		RaftMessage.AppendEntries request
	) throws Exception {
		return (RaftMessage.AppendEntriesResponse) sendRpc(
			peerHostPort,
			request
		);
	}

	private RaftMessage sendRpc(String peerHostPort, RaftMessage request)
		throws Exception {
		String host = peerHostPort.split(":")[0];
		int port = Integer.parseInt(peerHostPort.split(":")[1]);

		try (Socket socket = new Socket()) {
			socket.connect(new InetSocketAddress(host, port), 100);
			socket.setSoTimeout(100);

			mapper.writeValue(socket.getOutputStream(), request);
			return mapper.readValue(socket.getInputStream(), RaftMessage.class);
		}
	}

	@Override
	public void close() throws Exception {
		running.set(false);
		if (serverSocket != null) {
			serverSocket.close();
		}
	}
}
