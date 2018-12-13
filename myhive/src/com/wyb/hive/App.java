package com.wyb.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class App {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	public static void main(String[] args) throws Exception {
		Class.forName(driverName);
		Connection con = DriverManager.getConnection("jdbc:hive://192.168.56.104:10000/default", "", "");
		Statement stmt = con.createStatement();
		stmt.executeQuery("CREATE DATABASE mybase2");
	}
}
