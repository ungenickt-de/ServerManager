package com.playerrealms.servermanager.tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OSUtils {

	private static String OS = System.getProperty("os.name").toLowerCase();

	private OSUtils() {}
	
	public static boolean setProcessPriority(int pid, int priority) throws IOException {
		if(!isUnix()){
			return false;
		}
		
		Process process = Runtime.getRuntime().exec(new String[] {"renice", String.valueOf(priority), String.valueOf(pid)});
		
		try {
			process.waitFor(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	private static Map<String, Long> getMemoryData() throws IOException {
		if(!isUnix()){
			return Collections.emptyMap();
		}
		
		Process process = Runtime.getRuntime().exec(new String[] {"free", "-b"});
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), Charset.defaultCharset()));
		
		String[] names = reader.readLine().replaceAll("( +)"," ").trim().split(" ");
		
		String[] memNums = reader.readLine().replaceAll("( +)"," ").trim().split(" ");
		
		Map<String, Long> memory = new HashMap<>();
		
		int i = 1;
		
		for(String name : names){
			memory.put(name, Long.parseLong(memNums[i++]));
		}
		
		return memory;
		
	}
	
	@SuppressWarnings("restriction")
	public static long getMaxMemory(){
		if(isWindows()){
			return ((com.sun.management.OperatingSystemMXBean) ManagementFactory
			        .getOperatingSystemMXBean()).getTotalPhysicalMemorySize();
		}
		
		if(isUnix()){
			try {
				return getMemoryData().get("total");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return Integer.MAX_VALUE;
	}
	
	public static long getFreeMemory() {
		if(isWindows()){
			return ((com.sun.management.OperatingSystemMXBean) ManagementFactory
			        .getOperatingSystemMXBean()).getFreePhysicalMemorySize();
		}
		
		if(isUnix()){
			
			try {
				return getMemoryData().get("available");
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		
		return 0;
	}
	
	public static long mbToBytes(long mb){
		return mb * 1024 * 1024;
	}
	
	public static double byteToMegabyte(long b){
		return 1D * b / 1024D / 1024D;
	}
	
	public static boolean isWindows() {

		return (OS.indexOf("win") >= 0);

	}

	public static boolean isUnix() {

		return (OS.indexOf("nix") >= 0 || OS.indexOf("nux") >= 0 || OS.indexOf("aix") > 0 );

	}
	
}
