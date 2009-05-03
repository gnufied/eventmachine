package com.rubyeventmachine;

public class ClientDisconnectException extends RuntimeException { 
    private static final long serialVersionUID = -318337588650290552L;
	public String message;
    public ClientDisconnectException(String _message) {
    	message = _message;
	}
}
