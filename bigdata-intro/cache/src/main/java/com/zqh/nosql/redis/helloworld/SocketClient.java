package com.zqh.nosql.redis.helloworld;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SocketClient {

	private BlockingQueue<Request> requests = new LinkedBlockingQueue<Request>();
	private Charset charset = Charset.forName("utf-8");
	private Handler handler;

	SocketClient(String host, int port){
		handler = new Handler(host, port);
		handler.setDaemon(true);
		handler.start();
	}
	
	public void set(String key,String value){
		Request request = new Request(Command.SET, key,value);
		try{
			synchronized (request) {
				requests.put(request);
				request.wait();
			}
			Reply reply = request.reply;
			if(reply == null || reply.code == -1 || !reply.success){
				throw new RuntimeException("operation fail..");
			}
		}catch(InterruptedException e){
			return;
		}
	}	

	
	public String get(String key){
		Request request = new Request(Command.GET, key);
		try{
			synchronized (request) {
				requests.put(request);
				request.wait();
			}
			Reply reply = request.reply;
			if(reply == null || reply.code == -1 || !reply.success){
				throw new RuntimeException("operation fail..");
			}
			return reply.result;
			
		}catch(InterruptedException e){
			return null;
		}
	}
	
	public List<String> lrange(String key,int from,int to){
		Request request = new Request(Command.LRANGE, key,String.valueOf(from),String.valueOf(to));
		try{
			synchronized (request) {
				requests.put(request);
				request.wait();
			}
			Reply reply = request.reply;
			if(reply == null || reply.code == -1 || !reply.success){
				throw new RuntimeException("operation fail..");
			}
			return reply.lresult;
			
		}catch(InterruptedException e){
			return null;
		}
	}
	
	public Integer incr(String key){
		Request request = new Request(Command.INCR,key);
		try{
			synchronized (request) {
				requests.put(request);
				request.wait();
			}
			Reply reply = request.reply;
			if(reply == null || reply.code == -1 || !reply.success){
				throw new RuntimeException("operation fail..");
			}
			return Integer.valueOf(reply.result);
			
		}catch(InterruptedException e){
			return null;
		}
	}

	public void close(){
		handler.close();
	}

	static enum Command{
		GET,SET,LRANGE,INCR;
	}

	static class Request{
		Command command;
		String[] args;//协议的内容，需要发送的内容
		Thread blocker;//调用者线程
		Reply reply;
		Request(Command command,String... args){
			this.command = command;
			this.args = args;
			this.blocker = Thread.currentThread();
		}
	}
	
	static class Reply{
		String message;
		boolean success = false;
		int code = 0;
		String result;//all will be string
		List<String> lresult = new ArrayList<String>();//多个结果
		Reply(){}
		Reply(int code){
			this.code = code;
		}
	}
	
	class Handler extends Thread{
		Socket socket = null;
		boolean closed = false;
		BufferedReader is = null;
		OutputStream os = null;
		String host;
		int port;

		Handler(String host,int port){
			try{
				this.host = host;
				this.port = port;
				connect();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		
		private void connect() throws IOException{
			socket = new Socket();
			SocketAddress addr = new InetSocketAddress(host,port);
			socket.setKeepAlive(true);
			//socket.setSoTimeout(10000);
			socket.setSoLinger(true,0);
			//socket.setReceiveBufferSize(1024);
			socket.setTcpNoDelay(true);
			socket.connect(addr,10000); //blocking
			is = new BufferedReader(new InputStreamReader(socket.getInputStream(),charset));
			os = socket.getOutputStream();
		}
		
		public void close(){
			closed = true;
			this.interrupt();
		}

		private void write(Request request) throws IOException{
			os.write('*');
			String[] args = request.args;
			os.write(String.valueOf(args.length + 1).getBytes(charset));
			os.write('\r');
			os.write('\n');
			//*2
			os.write('$');
			byte[] cb = request.command.name().getBytes(charset);
			os.write(String.valueOf(cb.length).getBytes(charset));
			os.write('\r');
			os.write('\n');
			//$3
			os.write(cb);
			os.write('\r');
			os.write('\n');
			//GET
			for(String arg : args){
				byte[] ab = arg.getBytes(charset);
				os.write('$');
				os.write(String.valueOf(ab.length).getBytes(charset));
				os.write('\r');
				os.write('\n');
				os.write(ab);
				os.write('\r');
				os.write('\n');
			}
		}
		
		@Override
		public void run(){
			try{
				while(!closed){
					Request request = requests.take();
					try{
						write(request);
						char status = (char)is.read();
						Reply reply = new Reply();
						if(status != '-'){
							reply.success = true;
						}
						if(status == '+' || status == '-'){
							reply.message = read();
						}else if(status == '$'){
							reply.result = readString();
						}else if(status == '*'){
							reply.lresult = readMulti();
						}else if(status == ':'){
							reply.result = read();
						}else{
							request.reply = new Reply(-1);
							throw new RuntimeException("packet error..");
						}
						synchronized (request) {
							request.reply = reply;
							request.notifyAll();
						}
					}catch(Exception e){
						try{
							socket.close();
							this.connect();
						}catch(Exception ex){
							//
						}
						synchronized (request) {
							request.notifyAll();
						}
					}
				}
			}catch(InterruptedException e){
				try{
					closed = true;
					socket.close();
					for(Request request : requests){
						request.blocker.interrupt();
					}
					requests.clear();
				}catch(Exception ex){
					//
				}
			}
		}

		//read line
		private String read() throws IOException{
			StringBuilder sb = new StringBuilder();
			//\r\n必须互为成对
			//不能直接使用is.readline()
			boolean lfcr = false;
			while(true){
				char _char = (char)is.read();
				if(_char == -1){
					close();
					break;
				}
				//如果上一个字符为\r
				if(lfcr == true){
					if(_char == '\n'){
						break;
					}
					sb.append('\r');
					lfcr = false;
				}
				if(_char == '\r'){
					lfcr = true;
					continue;
				}
				sb.append(_char);
			}
			return sb.toString();
		}
		
		private List<String> readMulti() throws IOException{
			Integer size = Integer.valueOf(read());
			List<String> lresult = new ArrayList<String>();
			//eg: *3
			if(size > 0) {
				for(int i=0;i<size;i++){
					while(true){
						char _char = (char)is.read();//$3
						if(_char == '$'){
							lresult.add(readString());
							break;
						}
					}
				}
			}
			return lresult;
		}
		//such as:
		//$3
		//012
		private String readString() throws IOException{
			Integer size = Integer.valueOf(read());
			//-1 is null
			if(size > 0){
				return read();
			}
			return null;
		}
	}
}
