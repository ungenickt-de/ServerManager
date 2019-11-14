package com.playerrealms.servermanager;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.model.Filters;
import com.playerrealms.common.RedisConstants;
import com.playerrealms.common.ServerStatus;
import com.playerrealms.common.WorldGeneratorType;
import com.playerrealms.mctool.*;
import com.playerrealms.servermanager.redis.JedisAPI;
import com.playerrealms.servermanager.tool.WorldDownload;
import com.playerrealms.servermanager.yaml.Configuration;
import com.playerrealms.servermanager.yaml.ConfigurationProvider;
import com.playerrealms.servermanager.yaml.YamlConfiguration;
import org.apache.commons.io.FileUtils;
import org.bson.Document;
import org.zeroturnaround.zip.ZipUtil;

import java.io.*;
import java.net.ServerSocket;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Server extends ServerListener {

	public static final int DEFAULT_MAX_PLAYERS = 15;

	public static final int ULTRA_MAX_PLAYERS = 50;
	
	public static final int PREMIUM_MAX_PLAYERS = 25;
	
	private String name;
	
	private final ServerManager manager;
	
	private Logger logger;
	
	private MinecraftServer mc;
	
	private int port;
	
	private boolean restarting;
	
	private Thread checkingStartupThread;
	
	public Server(String name, ServerManager manager,Logger logger) {
		this.manager = manager;
		this.name = name;
		restarting = false;
		Document doc = findOurDoc();
		this.name = doc.getString("server_name");
		this.logger = logger;
		checkingStartupThread = null;
	}
	
	public void checkForStartup(){
		if(checkingStartupThread != null){
			if(checkingStartupThread.isAlive())
				checkingStartupThread.interrupt();
		}
		checkingStartupThread = new Thread(new Runnable() {
			
			@Override
			public void run() {

				logger.info("Waiting for startup ("+name+")");
				while(getOnlineStatus() == ServerStatus.STARTING){
					long lastLogUpdate = mc != null ? mc.getLastLogUpdate() : 0L;
					
					long time = System.currentTimeMillis() - lastLogUpdate;
					
					if(time > TimeUnit.MINUTES.toMillis(5)){
						try {
							logger.info("Did not startup after 5 minutes, forcing it closed ("+name+")");
							mc.stop(true);
							logger.info("Successfully closed ("+name+")");
						} catch (ServerNotOnlineException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}else{
						try {
							Thread.sleep(10000);
						} catch (InterruptedException e) {
						}
					}
				}
				logger.info("Finished waiting for startup "+getOnlineStatus()+" ("+name+")");
			}
		});
		checkingStartupThread.start();
		
	}
	
	public void setMc(MinecraftServer mc) {
		this.mc = mc;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public int getPort() {
		return port;
	}
	
	public boolean areCommandBlocksEnabled() {
		return getMetadata("cb", "on").equals("on");
	}
	
	public boolean areFlightEnabled(){
		return getMetadata("af", "on").equals("on");
	}
	
	public boolean isPremium(){
		if(!hasMetadata("premiumtime")){
			return false;
		}
		
		long time = Long.parseLong(getMetadata("premiumtime"));
		
		return time - System.currentTimeMillis() > 0;
	}
	
	public boolean isUltra(){
		if(!isPremium()){
			return false;
		}
		long time = Long.parseLong(getMetadata("ultra_time", "0"));
		
		if(time > 0) {
			if(time - System.currentTimeMillis() > 0) {
				return true;
			}
		}
		
		return getMetadata("ultra", "n").equals("y");
	}
	
	public int getMemoryUsage() {
		int ram = 1024;
		
		if(isPremium()){
			ram = 1536;
		}
		
		if(isUltra()){
			ram = 2048;
		}
		
		if(hasMetadata("ram")) {
			ram = Integer.parseInt(getMetadata("ram"));
		}

		ram = ram + 200; //For server stabilization
		return ram;
	}
	
	private Document findOurDoc() {
		MongoCollection<Document> col = manager.getMongoDatabase().getCollection("servers");
		
		FindIterable<Document> found = col.find(Filters.eq("server_name_lower", name.toLowerCase()));

		Document serverData = found.first();
		
		if(serverData == null) {
			throw new IllegalStateException("We aren't in the MongoDB!! ("+name+") ");
		}
		return serverData;
	}
	
	public boolean hasMetadata(String key) {
		Document serverData = findOurDoc();
		
		Document obj = (Document) serverData.get("metadata");
		
		return obj.containsKey(key);
	}
	
	public void setMetadata(String key, String value) {
		if(key == null || value == null) {
			throw new IllegalArgumentException("key or value cannot be null ("+key+") ("+value+")");
		}
		
		Document found = findOurDoc();
		
		MongoCollection<Document> col = manager.getMongoDatabase().getCollection("servers");
		
		if(value.isEmpty()) {
			col.findOneAndUpdate(Filters.eq(found.getObjectId("_id")), new Document("$unset", new Document("metadata."+key, "")));
		}else {
			col.findOneAndUpdate(Filters.eq(found.getObjectId("_id")), new Document("$set", new Document("metadata."+key, value)));	
		}
		
		announceChanges();
	}
	
	public String getMetadata(String key) {
		return getMetadata(key, null);
	}
	
	public String getMetadata(String key, String def) {
		
		Document serverData = findOurDoc();
		
		Document obj = (Document) serverData.get("metadata");
		
		if(!obj.containsKey(key)) {
			return def;
		}
		
		return obj.getString(key);
		
	}
	
	public void announceChanges() {
		JedisAPI.publish(RedisConstants.MANAGER_UPDATE_CHANNEL, name+" "+RedisConstants.UPDATE);
	}
	
	private void copyRequiredFiles() throws IOException {
		File folder = getFolder();
		File commons = new File("commons");
		
		FileUtils.copyDirectory(commons, folder);
		if(hasMetadata("type")) {
			File type = new File("servertypes/"+getMetadata("type"));
			if(type.exists()) {
				FileUtils.copyDirectory(type, folder);
			}else {
				logger.warning("Missing type folder for "+getMetadata("type"));
			}
		}
	}
	
	public boolean start() throws IOException, ServerAlreadyOnlineException {
		if(mc == null) {
			if(!downloadServerData()) {
				
				findOurDoc();//Make sure we are in the db
				
				logger.info("First time setup ("+name+")");

				if(name == null || name.isEmpty() || getFolder().getAbsolutePath().equals("/root/Manager/servers/")){
					throw new NullPointerException();
				}

				File folder = getFolder();
				if(folder.exists()) {
					//FileUtils.deleteDirectory(folder);
					File zipFile = new File("tmp/"+name+"_"+System.currentTimeMillis()+".zip");
					if(zipFile.exists()) {
						zipFile.delete();
					}
					ZipUtil.pack(folder, zipFile);
				}
				
				folder.mkdirs();
				
				copyRequiredFiles();
				
				logger.info("Finished first time setup ("+name+")");
				
			}else {
				copyRequiredFiles();
			}
			port = findFreePort();
			
			String generator = "";
			
			if(hasMetadata("wt")) {
				WorldGeneratorType worldType = WorldGeneratorType.getType(getMetadata("wt"));
				
				if(worldType == WorldGeneratorType.FLAT) {
					generator = "FLAT";
				}else if(worldType == WorldGeneratorType.NORMAL) {
					generator = "DEFAULT";
				}else if(worldType == WorldGeneratorType.VOID) {
					File bukkitSettings = new File(getFolder(), "bukkit.yml");
					Configuration yaml = ConfigurationProvider.getProvider(YamlConfiguration.class).load(bukkitSettings);
					yaml.set("worlds.world.generator", "PlayerRealms");
					ConfigurationProvider.getProvider(YamlConfiguration.class).save(yaml, bukkitSettings);
				}else if(worldType == WorldGeneratorType.UPLOAD) {
					
					String url = getMetadata("url");
					
					if(url != null) {
						setMetadata("url", "");
						
						URL website = new URL(url);
						
						File world = new File(getFolder(), "world");
						if(!world.exists()) {
							world.mkdir();
						}
						try {
							WorldDownload.downloadWorld(website, world, logger);
						}catch(Exception e) {
							e.printStackTrace();
						}
						
						
					}
					
				}
				
			}
			
			int max = DEFAULT_MAX_PLAYERS;
			
			if(isUltra()) {
				max = ULTRA_MAX_PLAYERS;
			}else if(isPremium()) {
				max = PREMIUM_MAX_PLAYERS;
			}
			
			if(hasMetadata("maxplayers")) {
				max = Integer.parseInt(getMetadata("maxplayers"));
			}
			
			//mc = new DirectMinecraftServer(new File(getFolder(), "spigot.jar"), new ServerSettings(port, getMemoryUsage(), true, areFlightEnabled(), areCommandBlocksEnabled(), generator, "-Dserver.name="+name, max, 1F).putEnvVar("servername", name));
			
			mc = new DockerServer(getFolder(), new ServerSettings(port, getMemoryUsage(), true, areFlightEnabled(), areCommandBlocksEnabled(), generator, "-Dserver.name="+name, max, 1F).putEnvVar("servername", name).putEnvVar("MAX_MEMORY",String.valueOf(getMemoryUsage())));
			
			mc.setListener(this);
		}
		
		if(mc.isRunning()) {
			throw new ServerAlreadyOnlineException();
		}
		
		mc.start();
		return true;
	}
	
	public void stop(boolean kill) throws ServerNotOnlineException, IOException {
		if(mc == null) {
			throw new ServerNotOnlineException();
		}
		
		if(mc.isRunning()) {
			setMetadata("STATUS", ServerStatus.STOPPING.name());
			try {
				mc.stop(kill);
			}catch(Exception e) {
				setMetadata("STATUS", ServerStatus.ONLINE.name());
				throw e;
			}
		}
	}
	
	public void restart() throws ServerNotOnlineException, IOException {
		if(mc == null) {
			throw new ServerNotOnlineException();
		}
		
		mc.restart();
	}
	
	public void consoleCommand(String cmd) throws ServerNotOnlineException, IOException {
		if(mc == null) {
			throw new ServerNotOnlineException();
		}
		mc.consoleCommand(cmd);
	}
	
	public File getFolder() {
		return new File("servers/"+name);
	}
	
	/**
	 * Download the servers data from MongoDB
	 * @return true if downloaded, false if not existent
	 * @throws IOException
	 */
	public boolean downloadServerData() throws IOException {

		GridFSBucket bucket = GridFSBuckets.create(manager.getMongoDatabase(), "server_files");
		
		GridFSFindIterable found = bucket.find(Filters.eq("filename", name+".zip"));
		
		GridFSFile fs_file = found.first();
		
		if(fs_file == null) {
			return false;
		}
		
		logger.info("Downloading "+fs_file.getFilename()+" "+fs_file.getUploadDate()+" "+fs_file.getMD5());
		
		File folder = getFolder();
		if(!folder.exists()) {
			folder.mkdirs();
		}
		
		File file = new File(getFolder(), fs_file.getFilename());
		if(!file.exists()) {
			file.createNewFile();
		}
		try(FileOutputStream fos = new FileOutputStream(file)){
			bucket.downloadToStream(fs_file.getObjectId(), fos);
		}
		
		ZipUtil.unpack(file, folder);
		
		file.delete();
		
		File plugins = new File(folder, "plugins");
		
		if(plugins.exists()) {
			
			for(File temp : plugins.listFiles(f -> f.getName().endsWith(".jar") && f.isFile())) {
				
				temp.renameTo(new File(plugins, temp.getName()+".temp"));
				
			}
			
		}

		for(File temp : plugins.listFiles(f -> f.getName().endsWith(".jar.temp") && f.isFile())) {
			
			String name = temp.getName().substring(0, temp.getName().indexOf(".temp"));
			
			File root = new File("commons/plugins/PlayerRealms/plugins");
			
			File replace = new File(root, name);
			
			if(replace.exists()) {
				FileUtils.copyFile(replace, new File(plugins, name));
			}
			temp.delete();
			
		}
		
		return true;
	}

	public static int findFreePort(){
		
		try(ServerSocket socket = new ServerSocket(0)){
			socket.setReuseAddress(true);
			int port = socket.getLocalPort();
			
			return port;
		}catch(IOException e){
			//Swallow
		}
		
		throw new IllegalStateException("Could not find a free port!");
	}
	
	@Override
	public void onServerRestartBegin(MinecraftServer server) {
		restarting = true;
	}
	
	@Override
	public void onServerRestartEnd(MinecraftServer server) {
		restarting = false;
	}
	
	@Override
	public void onServerStart(MinecraftServer server) {
		setMetadata("STATUS", ServerStatus.STARTING.name());
		setMetadata("SOURCE", Start.ip+":"+port);
		checkForStartup();
	}
	
	@Override
	public void onServerStop(MinecraftServer server) {
		if(restarting) {
			setMetadata("STATUS", ServerStatus.STARTING.name());
			try {
				copyRequiredFiles();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return;
		}
		if(hasMetadata("autorestart")) {
			if(getMetadata("autorestart").equals("true")) {
				try {
					start();
				} catch (IOException | ServerAlreadyOnlineException e) {
					e.printStackTrace();
				}
				return;
			}
		}
		setMetadata("STATUS", ServerStatus.STOPPING.name());
		try {
			File folder = getFolder();
			
			logger.info("Uploading server to MongoDB ("+name+")");
			
			uploadServer(getName(), folder, manager);
			
			logger.info("Finished upload ("+name+")");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			setMetadata("STATUS", ServerStatus.OFFLINE.name());
			setMetadata("SOURCE", "");
			mc = null;
		}
	}

	public boolean isRunning() {
		if(mc == null) {
			return false;
		}
		return restarting || mc.isRunning();
	}
	
	public ServerStatus getOnlineStatus() {
		return ServerStatus.valueOf(getMetadata("STATUS", ServerStatus.OFFLINE.name()));
	}
	
	public static void uploadServer(String serverName, File folder, ServerManager manager) throws IOException {
		File spigotJar = new File(folder, "spigot.jar");
		spigotJar.delete();
		File plugins = new File(folder, "plugins/PlayerRealms/plugins");
		FileUtils.deleteDirectory(plugins);

		File coreFile = new File(folder, "core");
		if(coreFile.exists()){
			coreFile.delete();
		}
		
		plugins = new File(folder, "plugins");
		
		if(plugins.exists()) {
			for(File pl : plugins.listFiles(f -> f.getName().endsWith(".jar") && f.isFile())) {
				
				String name = pl.getName();
				
				pl.delete();
				
				File temp = new File(plugins, name+".temp");
				
				temp.createNewFile();
				
			}
		}
		
		File zipFile = new File(serverName+".zip");
		if(zipFile.exists()) {
			zipFile.delete();
		}
		
		ZipUtil.pack(folder, zipFile);
		
		GridFSBucket bucket = GridFSBuckets.create(manager.getMongoDatabase(), "server_files");

		for(GridFSFile file : bucket.find(Filters.eq("filename", serverName+".zip"))) {
			bucket.delete(file.getObjectId());
		}
		
		try(FileInputStream fis = new FileInputStream(zipFile)){
			bucket.uploadFromStream(zipFile.getName(), fis);
		}
		
		FileUtils.deleteDirectory(folder);
		zipFile.delete();
	}
	
}
