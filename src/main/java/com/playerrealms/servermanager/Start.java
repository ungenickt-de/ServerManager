package com.playerrealms.servermanager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;

public class Start {

	public static String ip = "";
	public static final int DEFAULT_PORT = 8484;
	
	public static void main(String[] args) throws IOException {
		
		ip = getIP();
		
		boolean downloadGlobal = true;
		boolean fixNames = false;
		boolean disallow = false;
		
		for(int i = 0; i < args.length;i++) {
			if(args[i].equalsIgnoreCase("-skipDownloads")) {
				downloadGlobal = false;
			}else if(args[i].equalsIgnoreCase("-fixNames")) {
				fixNames = true;
			}else if(args[i].equalsIgnoreCase("-block")) {
				disallow = true;
			}
		}
		
		ServerManager manager = new ServerManager();
		
		manager.setup(downloadGlobal);
		
		if(fixNames) {
			manager.renameBadServers();
		}
		
		if(disallow) {
			manager.changeAcceptingServers(false);
		}else{
			manager.changeAcceptingServers(true);
		}
		
		manager.run();
	}

	private static String getIP(){
		if(!ip.isEmpty()){
			return ip;
		}
		try {
			URL whatismyip = new URL("http://checkip.amazonaws.com");
			BufferedReader in = new BufferedReader(new InputStreamReader(
			                whatismyip.openStream()));

			return in.readLine();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "127.0.0.1";
	}
	
}
