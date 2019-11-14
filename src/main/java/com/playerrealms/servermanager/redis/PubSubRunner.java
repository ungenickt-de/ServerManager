package com.playerrealms.servermanager.redis;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.zeroturnaround.zip.ZipException;

import com.playerrealms.common.RedisConstants;
import com.playerrealms.common.ResponseCodes;
import com.playerrealms.common.ServerInformation;
import com.playerrealms.mctool.ServerAlreadyOnlineException;
import com.playerrealms.mctool.ServerNotOnlineException;
import com.playerrealms.servermanager.ServerManager;
import com.playerrealms.servermanager.Start;

import redis.clients.jedis.JedisPubSub;

public class PubSubRunner extends JedisPubSub implements Runnable {

	private ServerManager manager;
	
	private Map<String, Thread> taskContext;
	
	public PubSubRunner(ServerManager manager) {
		this.manager = manager;
		taskContext = Collections.synchronizedMap(new HashMap<>());
	}
	
	@Override
	public void onMessage(String channel, String message) {
		try {
			handleMessage(message);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void doAction(String ctx, Runnable run, Runnable fail) {
		if(taskContext.containsKey(ctx)) {
			fail.run();
		}else {
			Thread t = new Thread(() ->  {
				try {
					run.run();
				}finally {
					taskContext.remove(ctx);
				}
			});
			t.setDaemon(true);
			t.start();
			t.setName(ctx+" Action");
			taskContext.put(ctx, t);
		}
		
	}
	
	private void handleMessage(String message) {
		System.out.println("A message came in -> "+message);
		String[] cmd = message.split(" ");
		
		String reqId = cmd[1];
		
		if(cmd[0].equals(RedisConstants.START_SERVER)) {
			doAction(cmd[2], () -> {
				try {
					
					if(!manager.isAccepting()){
						response(reqId, ResponseCodes.MEMORY_LIMIT_REACHED);
					}else if(manager.startServer(cmd[2])) {
						response(reqId, ResponseCodes.SERVER_STARTING);
					}else {
						response(reqId, ResponseCodes.UNKNOWN_SERVER);
					}
					
				} catch (IOException e) {
					e.printStackTrace();
					response(reqId, ResponseCodes.UNKNOWN_ERROR);
				} catch (ZipException e) {
					e.printStackTrace();
					response(reqId, ResponseCodes.UNKNOWN_ERROR);
				} catch (ServerAlreadyOnlineException e) {
					response(reqId, ResponseCodes.SERVER_ALREADY_RUNNING);
				} catch(OutOfMemoryError e) {
					e.printStackTrace();
					response(reqId, ResponseCodes.UNKNOWN_ERROR);
				}
			}, () -> response(reqId, ResponseCodes.SERVER_ALREADY_RUNNING));
		}else if(cmd[0].equals(RedisConstants.STOP_SERVER)) {
			doAction(cmd[2], new Runnable() {
				
				@Override
				public void run() {
					try {
						boolean force = Boolean.parseBoolean(cmd[3]);
						if(manager.stopServer(cmd[2], force)) {
							if(force)
								response(reqId, ResponseCodes.SERVER_FORCE_STOPPED);
							else
								response(reqId, ResponseCodes.SERVER_STOPPED);
						}else {
							response(reqId, ResponseCodes.UNKNOWN_SERVER);
						}
					} catch (ServerNotOnlineException e) {
						response(reqId, ResponseCodes.SERVER_NOT_RUNNING);
					} catch (IOException e) {
						e.printStackTrace();
						response(reqId, ResponseCodes.UNKNOWN_ERROR);
					}
				}
			}, () -> response(reqId, ResponseCodes.SERVER_STOPPED));
		}else if(cmd[0].equals(RedisConstants.DELETE_SERVER)) {
			doAction(cmd[2], () -> {
				try {
					if(manager.deleteServer(cmd[2], false)) {
						response(reqId, ResponseCodes.SERVER_REMOVED);
					}else{
						response(reqId, ResponseCodes.UNKNOWN_SERVER);
					}
				} catch (ServerAlreadyOnlineException e) {
					e.printStackTrace();
					response(reqId, ResponseCodes.SERVER_ALREADY_RUNNING);
				}
			}, () -> {
				response(reqId, ResponseCodes.SERVER_ALREADY_RUNNING);
			});
			
		}else if(cmd[0].equals(RedisConstants.DELETE_DATA)) {
			doAction(cmd[2], () -> {
			
				try {
					if(manager.deleteServer(cmd[2], true)) {
						response(reqId, ResponseCodes.SERVER_REMOVED);
					}else{
						response(reqId, ResponseCodes.UNKNOWN_SERVER);
					}
				} catch (ServerAlreadyOnlineException e) {
					e.printStackTrace();
					response(reqId, ResponseCodes.SERVER_ALREADY_RUNNING);
				}
			
			}, () -> {
				response(reqId, ResponseCodes.SERVER_ALREADY_RUNNING);
			});
		}else if(cmd[0].equals(RedisConstants.RESTART_SERVER)) {
			doAction(cmd[2], new Runnable() {
				
				@Override
				public void run() {
					try {
						if(manager.restartServer(cmd[2])) {
							response(reqId, ResponseCodes.SERVER_RESTARTING);
						}else{
							response(reqId, ResponseCodes.UNKNOWN_SERVER);
						}
					} catch (ServerNotOnlineException e) {
						response(reqId, ResponseCodes.SERVER_NOT_RUNNING);
					} catch (IOException e) {
						e.printStackTrace();
						response(reqId, ResponseCodes.UNKNOWN_ERROR);
					}
				}
			}, () -> response(reqId, ResponseCodes.SERVER_NOT_RUNNING));
		}else if(cmd[0].equals(RedisConstants.SET_METADATA)) {
			String val = "";
			for(int i = 4; i < cmd.length;i++) {
				val += cmd[i] + " ";
			}
			if(manager.alterMetadata(cmd[2], cmd[3], val)) {
				response(reqId, ResponseCodes.METADATA_SET);
			}else {
				response(reqId, ResponseCodes.UNKNOWN_SERVER);
			}
		}else if(cmd[0].equals(RedisConstants.COMMAND_SERVER)) {
			String val = "";
			for(int i = 3; i < cmd.length;i++) {
				val += cmd[i] + " ";
			}
			try {
				if(manager.consoleCommand(cmd[2], val)) {
					response(reqId, ResponseCodes.METADATA_SET);
				}else {
					response(reqId, ResponseCodes.UNKNOWN_SERVER);
				}
			} catch (IOException e) {
				e.printStackTrace();
				response(reqId, ResponseCodes.UNKNOWN_ERROR);
			} catch (ServerNotOnlineException e) {
				response(reqId, ResponseCodes.SERVER_NOT_RUNNING);
			}
		}else if(cmd[0].equals(RedisConstants.CREATE_SERVER)) {
			String name = cmd[2];
			String type = cmd[3];
			
			doAction(name, () -> {
				if(ServerInformation.validateName(name)) {
					if(manager.createServer(name, type)) {
						response(reqId, ResponseCodes.SERVER_CREATED);
					}else {
						response(reqId, ResponseCodes.SERVER_NAME_TAKEN);
					}
				}else {
					response(reqId, ResponseCodes.SERVER_NAME_INVALID);
				}
			}, () -> {
				response(reqId, ResponseCodes.SERVER_NAME_TAKEN);
			});
		}else if(cmd[0].equals(RedisConstants.RENAME_SERVER)) {
			String name = cmd[2];
			String target = cmd[3];
			
			doAction(name, () -> {
				if(!ServerInformation.validateName(target)) {
					response(reqId, ResponseCodes.SERVER_NAME_INVALID);
				}else {
					try {
						if(manager.renameServer(name, target)) {
							response(reqId, ResponseCodes.SERVER_RENAMED);
						} else {
							response(reqId, ResponseCodes.SERVER_NAME_TAKEN);
						}
					} catch (ServerAlreadyOnlineException e) {
						response(reqId, ResponseCodes.UNKNOWN_ERROR);
					}
				}
			}, () -> {
				response(reqId, ResponseCodes.UNKNOWN_ERROR);
			});
			
			
			
		}else if(cmd[0].equals(RedisConstants.NEW_GLOBAL_FILE)){
			try {
				manager.downloadGlobalData();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}else if(cmd[0].equals(RedisConstants.REGISTER)){
			try {
				Runtime.getRuntime().exec("ufw allow from "+cmd[1]);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void response(String id, ResponseCodes code) {
		JedisAPI.publish(RedisConstants.MANAGER_RESPONSE_CHANNEL, id+" "+code);
	}
	
	@Override
	public void run() {
		JedisAPI.subscribe(this, RedisConstants.MANAGER_REQUEST_CHANNEL+Start.ip, RedisConstants.MANAGER_GLOBAL_CHANNEL);
	}
	
	
}
