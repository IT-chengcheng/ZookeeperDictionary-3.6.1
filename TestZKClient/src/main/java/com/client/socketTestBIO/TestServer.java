package com.client.socketTestBIO;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class TestServer {

	public static void main(String[] args) {

		try {
            // 新建服务端对象，指定一个端口
			ServerSocket serverscoket = new ServerSocket(8888);

			System.out.println("server启动");
			// 开启监听，等待连接
			Socket server = serverscoket.accept();
			System.out.println("server收到连接："+ server.getInetAddress().getHostAddress());
			// 发送消息给客户端
			OutputStream os = server.getOutputStream();

			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));

			String msg = "你好啊！ ip:" + server.getInetAddress().getHostAddress() + "\n";

			bw.write(msg);
			bw.flush();

			// 接收客户端发过来的内容
			InputStream is = server.getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));

			System.out.println("readLine之前 ->>>>>>>");

			String readLine = br.readLine();

			System.out.println("来自客户端的消息："+readLine);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
