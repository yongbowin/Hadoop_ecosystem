package com.wyb.flume;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

/**
 * 
 * ��;������TCP����Ϣ�����������s0��
 * Administrator:yongbo.wang
 * ClassName:com.wyb.flume.App
 * 2017��1��20�� ����10:42:23
 *
 */
public class App {
	public static void main(String[] args) throws Exception {
		Socket socket = new Socket("localhost", 5140);
		OutputStream os = socket.getOutputStream();
		String str = "hello world!\nhow are you?\n";
		os.write(str.getBytes());
		os.flush();
		os.close();
		socket.close();
		System.out.println("over");
	}
}
