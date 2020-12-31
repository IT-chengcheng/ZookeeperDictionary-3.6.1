package com.client.socketTestBIO;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

public class TestClient {

	public static void main(String[] args) {

		// 新建Socket对象

		String ip = "127.0.0.1";
        int port = 8888;

		try {
			Socket clinet = new Socket(ip, port);
			System.out.println("连接成功 ->>>>>>");
			// 接收服务端发过来的信息

			InputStream is = clinet.getInputStream();

			BufferedReader br = new BufferedReader(new InputStreamReader(is));

			String str = br.readLine();

			System.out.println("服务器发过来的信息：" + str);

			// 发消息给服务端
			OutputStream os = clinet.getOutputStream();

			String msg = "我是客户端" + "\n";

			os.write(msg.getBytes());

			os.flush();

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}