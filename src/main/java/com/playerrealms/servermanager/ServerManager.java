package com.playerrealms.servermanager;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.model.Filters;
import com.playerrealms.common.RedisConstants;
import com.playerrealms.common.ServerInformation;
import com.playerrealms.common.ServerStatus;
import com.playerrealms.mctool.ServerAlreadyOnlineException;
import com.playerrealms.mctool.ServerNotOnlineException;
import com.playerrealms.servermanager.logging.CustomFormatter;
import com.playerrealms.servermanager.redis.JedisAPI;
import com.playerrealms.servermanager.redis.PubSubRunner;
import com.playerrealms.servermanager.tool.FileUtils;
import com.playerrealms.servermanager.tool.OSUtils;
import com.playerrealms.servermanager.yaml.Configuration;
import com.playerrealms.servermanager.yaml.YamlConfiguration;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.zeroturnaround.zip.ZipUtil;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;
import java.util.function.Consumer;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ServerManager implements Runnable {

	private MongoClient mongoClient;
	private MongoDatabase mongoDatabase;
	
	private Configuration config;
	
	private Logger logger;
	
	private Map<String, Server> servers;

	private int maxservers = 100;

	private boolean shuttingDown = false;
	
	private Thread rThread;
	
	private boolean accepting;
	
	public void setup(boolean downloadFromMongo) throws IOException {
		logger = Logger.getLogger("Player Realms");
		FileHandler fileHandler = new FileHandler("./manager.log", true);
		fileHandler.setFormatter(new CustomFormatter());
		logger.addHandler(fileHandler);
		logger.setLevel(Level.INFO);
		ConsoleHandler consoleHandler = new ConsoleHandler();
		consoleHandler.setFormatter(new CustomFormatter());
		consoleHandler.setLevel(Level.INFO);
		logger.addHandler(consoleHandler);
		logger.setUseParentHandlers(false);
		logger.info("Our IP is "+Start.ip);
		logger.info("Downloading config...");
		config = downloadConfig();
		logger.info("Connecting to Redis");
		String redisIp = config.getString("redis_new");
		if(redisIp.equals(Start.ip)) {
			redisIp = "127.0.0.1";
		}
		JedisAPI.init(redisIp, 6379, config.getString("redis_password"));
		
		logger.info("Connecting to Mongo...");
		mongoClient = new MongoClient(config.getString("mongo").equals(Start.ip) ? "127.0.0.1" : config.getString("mongo"), 27017);
		mongoDatabase = mongoClient.getDatabase("playerrealms");
		if(downloadFromMongo) {
			logger.info("Downloading global info...");
			downloadGlobalData();
			logger.info("Finished downloading");	
		}
		servers = Collections.synchronizedMap(new TreeMap<String, Server>(String.CASE_INSENSITIVE_ORDER));
		
		fixBrokenServers();
		
		PubSubRunner runner = new PubSubRunner(this);
		rThread = new Thread(runner);
		rThread.setName("PubSubRunner");
		rThread.setDaemon(true);
		rThread.start();
		
		Thread updater = new Thread(new Runnable() {
			
			@Override
			public void run() {
				while(true) {
					try {
						updateServerPresence();
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch(Exception e){
						e.printStackTrace();
					}
				}
			}
		});
		updater.setName("MongoUpdater");
		updater.setDaemon(true);
		updater.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			
			@Override
			public void run() {
				if(shuttingDown) {
					return;
				}
				
				logger.info("Turning off all servers");
				
				turnOffAllServers();
				
				List<ThreadInfo> threads = Arrays.asList(ManagementFactory.getThreadMXBean().dumpAllThreads(true, true));
			
				System.out.println("Dumping threads.");
				
				for(ThreadInfo info : threads){
					System.out.println(info);
				}
				
				File file = new File("crash_"+System.currentTimeMillis()+".log");
				
				try {
					file.createNewFile();
					System.out.println("Dumping threads into "+file.getName());
					
					try(FileOutputStream fos = new FileOutputStream(file)){
						try(BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fos))){
							
							for(ThreadInfo info : threads){
								out.write(info.toString());
								out.newLine();
							}
							
							out.flush();
						}
					}
					
					
				} catch (IOException e) {
					e.printStackTrace();
				}
				
			}
			
		}));
		
	}
	
	public void turnOffAllServers() {
		for(Server server : new ArrayList<>(servers.values())) {
			if(server.isRunning()) {
				logger.info("Shutting down "+server.getName());
				Thread t = new Thread(new Runnable() {
					
					@Override
					public void run() {
						try {
							server.stop(false);
						} catch (ServerNotOnlineException | IOException e) {
							e.printStackTrace();
						}
					}
				});
				t.setName(server.getName()+"-Shutdown");
				t.start();
			}
		}
	}
	
	public void renameBadServers() {
		
		MongoCollection<Document> col = mongoDatabase.getCollection("servers");
		Random r = new Random();
		for(Document server : col.find()) {
			
			String name = server.getString("server_name");
			if(!ServerInformation.validateName(name)) {
				logger.warning("Invalid name "+name+" ");
				try {
					String n = "\u30EA\u30CD\u30FC\u30E0-"+r.nextInt(99999);
					renameServer(name, n);
					logger.info("Changed to "+n);
				} catch (ServerAlreadyOnlineException e) {
					e.printStackTrace();
				}
			}
			
		}
		
	}
	
	private void fixBrokenServers() {
		
		MongoCollection<Document> col = mongoDatabase.getCollection("servers");
		
		logger.info("Fixing broken servers...");

		for(Document server : col.find(Filters.exists("metadata.SOURCE"))) {
			
			Document meta = (Document) server.get("metadata");

			String ip = meta.getString("SOURCE");

			if(ip == null) {
				continue;
			}
			
			ip = ip.split(":")[0];
			
			File us = new File("servers/"+server.getString("server_name"));
			
			if(ip.equals(Start.ip)) {
				col.findOneAndUpdate(Filters.eq(server.getObjectId("_id")), new Document("$unset", 
						new Document("metadata.SOURCE", "")
						.append("metadata.STATUS", "")));
				
				//File us = new File("servers/"+server.getString("server_name"));
				
				if(us.exists() && us.isDirectory()) {
					logger.info("We have the files for "+us.getPath()+", uploading to mongo...");
					
					try {
						Server.uploadServer(server.getString("server_name"), us, this);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
			}
		}
	}

	@Override
	public void run() {
		logger.info("MyIP "+Start.ip);
		try(Scanner scanner = new Scanner(System.in)){
			
			while(true) {
				try {
					System.out.println("> ");
					String line = scanner.nextLine();
					String args[] = line.split(" ");
					if(args[0].equalsIgnoreCase("create")) {
						String name = args[1];
						String type = args[2];
						
						if(createServer(name, type)) {
							logger.info("Created server "+name);
						}else {
							logger.info("Name taken");
						}
					}else if(args[0].equalsIgnoreCase("delete")) {
						String name = args[1];
						
						try {
							if(deleteServer(name, false)) {
								logger.info("Deleted "+name);
							}else {
								logger.info("Doesn't exist");
							}
						} catch (ServerAlreadyOnlineException e) {
							e.printStackTrace();
						}
					}else if(args[0].equalsIgnoreCase("start")) {
						String name = args[1];
						
						try {
							if(!startServer(name)){
								logger.warning("Could not find server with name "+name);
							}else{
								logger.info("Server started");
							}
						} catch(OutOfMemoryError e) {
							logger.info("Not enough RAM!");
						} catch (IOException | ServerAlreadyOnlineException e) {
							e.printStackTrace();
						}
						
					}else if(args[0].equalsIgnoreCase("stop")) {
						String name = args[1];
						
						try {
							stopServer(name, false);
						} catch (ServerNotOnlineException | IOException e) {
							e.printStackTrace();
						}
					}else if(args[0].equalsIgnoreCase("cmd")) {
						String name = args[1];
						String cmd = "";
						for(int i = 2; i < args.length;i++)
							cmd += args[i] + " ";
						try {
							consoleCommand(name, cmd);
						} catch (ServerNotOnlineException | IOException e) {
							e.printStackTrace();
						}
					}else if(args[0].equalsIgnoreCase("restart")) {
						
						String name = args[1];
						
						try {
							restartServer(name);
						} catch (ServerNotOnlineException | IOException e) {
							e.printStackTrace();
						}
					}else if(args[0].equalsIgnoreCase("set")) {
						String name = args[1];
						String key = args[2];
						String val = args[3];
						
						alterMetadata(name, key, val);
					}else if(args[0].equalsIgnoreCase("rename")) {
						String name = args[1];
						String target = args[2];
						
						try {
							renameServer(name, target);
						} catch (ServerAlreadyOnlineException e) {
							e.printStackTrace();
						}
					}else if(args[0].equalsIgnoreCase("regrab")){
						logger.info("Grabbing global files");
						try {
							downloadGlobalData();
						} catch (IOException e) {
							e.printStackTrace();
						}
						logger.info("Finished downloading global files.");
					}else if(args[0].equalsIgnoreCase("global")) {
						String path = args[1];
						File file = new File(path);
						
						GridFSBucket bucket = GridFSBuckets.create(mongoDatabase, "global_files");

						try(FileInputStream fis = new FileInputStream(file)) {
							logger.info("Uploading...");
							bucket.find(Filters.eq("filename", file.getName())).forEach(new Consumer<GridFSFile>() {
								@Override
								public void accept(GridFSFile fs_file) {
									bucket.delete(fs_file.getObjectId());
								}
							});
							ObjectId id = bucket.uploadFromStream(file.getName(), fis);
							logger.info("Uploaded to "+id);
							
							JedisAPI.publish(RedisConstants.MANAGER_GLOBAL_CHANNEL, RedisConstants.NEW_GLOBAL_FILE+" 0");
							
						} catch (FileNotFoundException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}else if(args[0].equalsIgnoreCase("supload")) {
						String path = args[1];
						File file = new File(path);
						
						GridFSBucket bucket = GridFSBuckets.create(mongoDatabase, "server_files");

						try(FileInputStream fis = new FileInputStream(file)) {
							logger.info("Uploading...");
							bucket.find(Filters.eq("filename", file.getName())).forEach(new Consumer<GridFSFile>() {
								@Override
								public void accept(GridFSFile fs_file) {
									bucket.delete(fs_file.getObjectId());
								}
							});
							ObjectId id = bucket.uploadFromStream(file.getName(), fis);
							logger.info("Uploaded to "+id);
						} catch (FileNotFoundException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}else if(args[0].equalsIgnoreCase("disallow")) {
						changeAcceptingServers(false);
					}else if(args[0].equalsIgnoreCase("allow")) {
						changeAcceptingServers(true);
					}else if(args[0].equalsIgnoreCase("dump")) {
						List<ThreadInfo> threads = Arrays.asList(ManagementFactory.getThreadMXBean().dumpAllThreads(true, true));
						
						for(ThreadInfo info : threads){
							System.out.println(info);
						}
						
						File file = new File("crash_"+System.currentTimeMillis()+".log");
						
						try {
							file.createNewFile();
							System.out.println("Dumping threads into "+file.getName());
							
							try(FileOutputStream fos = new FileOutputStream(file)){
								try(BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fos))){
									
									for(ThreadInfo info : threads){
										out.write(info.toString());
										out.newLine();
									}
									
									out.flush();
								}
							}
							
							
						} catch (IOException e) {
							e.printStackTrace();
						}
					}else if(args[0].equalsIgnoreCase("exit")) {
						shuttingDown = true;
						deletePresence();
						
						turnOffAllServers();
						
						break;
						
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}
			
		}
	}
	
	public void updateServerPresence() {
		if(shuttingDown) {
			return;
		}
		MongoCollection<Document> managerCollection = mongoDatabase.getCollection("managers");
		
		long free = 0;
		
		try{
			free = OSUtils.getFreeMemory();
		}catch(Exception e){
			e.printStackTrace();
		}
		
		Document first = managerCollection.findOneAndUpdate(Filters.eq("ip", Start.ip), new Document()
				.append("$set", 
						new Document("port", Start.DEFAULT_PORT)
						.append("free", free)
						.append("time", System.currentTimeMillis()))
				);
		
		if(first == null) {
			Document doc = new Document()
					.append("ip", Start.ip)
					.append("port", Start.DEFAULT_PORT)
					.append("free", free)
					.append("time", System.currentTimeMillis());
			managerCollection.insertOne(doc);
			changeAcceptingServers(true);
			logger.info("Created us in database since it was our first time");
		}
	}
	
	public void deletePresence() {
		MongoCollection<Document> managerCollection = mongoDatabase.getCollection("managers");
		
		managerCollection.findOneAndDelete(Filters.eq("ip", Start.ip));
	}
	
	public Document getServerData(String name) {
		MongoCollection<Document> col = mongoDatabase.getCollection("servers");
		
		FindIterable<Document> found = col.find(Filters.eq("server_name_lower", name.toLowerCase()));

		Document serverData = found.first();
		
		if(serverData == null) {
			return null;
		}
		
		return serverData;
	}
	
	public boolean stopServer(String name, boolean kill) throws ServerNotOnlineException, IOException {
		if(servers.containsKey(name)) {
			Server server = servers.get(name);
			
			server.stop(kill);
			updateServerPresence();
			return true;
		}else {
			return false;
		}
	}
	
	public boolean renameServer(String name, String target) throws ServerAlreadyOnlineException {
		boolean isOnline = false;
		MongoCollection<Document> col = getMongoDatabase().getCollection("servers");
	
		if(servers.containsKey(name)) {
			Server server = servers.get(name);
			isOnline = server.isRunning();
		}else {
			
			FindIterable<Document> found = col.find(Filters.eq("server_name_lower", name.toLowerCase()));

			Document serverData = found.first();
			
			if(serverData == null) {
				return false;
			}
			
			serverData = (Document) serverData.get("metadata");
			
			if(serverData.containsKey("STATUS")) {
				isOnline = !serverData.getString("STATUS").equals("OFFLINE");
			}
			
			
		}
		
		if(isOnline) {
			throw new ServerAlreadyOnlineException();
		}
		
		if(col.find(Filters.eq("server_name_lower", target.toLowerCase())).first() != null) {
			return false;
		}
		
		col.findOneAndUpdate(Filters.eq("server_name_lower", name.toLowerCase()), 
				new Document()
				.append("$set", new Document("server_name", target).append("server_name_lower", target.toLowerCase()))
				);
		GridFSBucket bucket = GridFSBuckets.create(getMongoDatabase(), "server_files");

		GridFSFindIterable found = bucket.find(Filters.eq("filename", name+".zip"));
		
		if(found.first() != null) {
			bucket.rename(found.first().getObjectId(), target+".zip");	
		}
		
		JedisAPI.publish(RedisConstants.MANAGER_UPDATE_CHANNEL, name+" "+RedisConstants.DELETE);
		JedisAPI.publish(RedisConstants.MANAGER_UPDATE_CHANNEL, target+" "+RedisConstants.UPDATE);
		
		return true;
	}
	
	public boolean consoleCommand(String name, String cmd) throws ServerNotOnlineException, IOException {
		if(servers.containsKey(name)) {
			Server server = servers.get(name);
			
			server.consoleCommand(cmd);
			return true;
		}else {
			return false;
		}
	}
	
	public boolean alterMetadata(String name, String key, String val) {
		if(servers.containsKey(name)) {
			Server server = servers.get(name);
			
			server.setMetadata(key, val);
			return true;
		}else {
			return false;
		}
	}
	
	public boolean isAccepting() {
		return accepting;
	}
	
	public void changeAcceptingServers(boolean accepting) {
		this.accepting = accepting;
		MongoCollection<Document> managerCollection = mongoDatabase.getCollection("managers");
		
		managerCollection.findOneAndUpdate(Filters.eq("ip", Start.ip), new Document()
				.append("$set", 
						new Document("port", Start.DEFAULT_PORT)
						.append("free", OSUtils.getFreeMemory())
						.append("time", System.currentTimeMillis())
						.append("accept", accepting))
				);
	}
	
	public boolean restartServer(String name) throws ServerNotOnlineException, IOException {
		if(servers.containsKey(name)) {
			Server server = servers.get(name);
			
			server.restart();
			return true;
		}else {
			return false;
		}
	}
	
	public boolean startServer(String name) throws IOException, ServerAlreadyOnlineException {
		
		Document doc = getServerData(name);
		
		if(doc != null){
			Server server = null;
			boolean isNew = false;
			if(servers.containsKey(name)) {
				server = servers.get(name);
			}else {
				server = new Server(name, this,logger);
				isNew = true;
			}
			
			if(server.getOnlineStatus() != ServerStatus.OFFLINE) {
				throw new ServerAlreadyOnlineException();
			}
			
			if(OSUtils.getFreeMemory() < server.getMemoryUsage() + 8192) {
				throw new OutOfMemoryError();
			}
			
			server.start();
			updateServerPresence();
			
			if(isNew) {
				servers.put(name.toLowerCase(), server);
			}
			
			return true;
		}
		
		return false;
	}
	
	public boolean deleteServer(String name, boolean justData) throws ServerAlreadyOnlineException {
		Server server = null;
		boolean ours = false;
		if(!servers.containsKey(name)) {
			try {
				server = new Server(name, this,logger);
			}catch(Exception e) {
				return false;
			}
		}else {
			server = servers.get(name);
			ours = true;
		}
		if(server.hasMetadata("STATUS") && !server.getMetadata("STATUS").equals(ServerStatus.OFFLINE.name()) && !ours) {
			throw new ServerAlreadyOnlineException();
		}
		try {
			if(server.isRunning())
				server.stop(true);
			
			MongoCollection<Document> col = mongoDatabase.getCollection("servers");
			
			Document deleted;
			
			if(justData) {
				deleted = col.find(Filters.eq("server_name_lower", name.toLowerCase())).first();
			}else {
				deleted = col.findOneAndDelete(Filters.eq("server_name_lower", name.toLowerCase()));
			}
			
			if(deleted == null) {
				return false;
			}
			
			GridFSBucket bucket = GridFSBuckets.create(mongoDatabase, "server_files");
			
			for(GridFSFile file : bucket.find(Filters.eq("filename", deleted.getString("server_name")+".zip"))) {
				bucket.delete(file.getObjectId());
			}
			
			JedisAPI.publish(RedisConstants.MANAGER_UPDATE_CHANNEL, name+" "+RedisConstants.DELETE);
			
		} catch (ServerNotOnlineException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		
		return true;
	}
	
	public boolean createServer(String name, String type) {
		Document doc = getServerData(name);
		if(doc != null) {
			return false;
		}
		MongoCollection<Document> col = mongoDatabase.getCollection("servers");
		Document serverData = new Document()
				.append("server_name", name)
				.append("server_name_lower", name.toLowerCase())
				.append("metadata", new BasicDBObject().append("type", type));
		
		col.insertOne(serverData);
		
		JedisAPI.publish(RedisConstants.MANAGER_UPDATE_CHANNEL, name+" "+RedisConstants.UPDATE);
		
		return true;
	}
	
	
	
	public MongoDatabase getMongoDatabase() {
		return mongoDatabase;
	}
	
	public void downloadGlobalData() throws FileNotFoundException, IOException {

		GridFSBucket bucket = GridFSBuckets.create(mongoDatabase, "global_files");
		
		for(GridFSFile fs_file : bucket.find()) {
			
			logger.info("Downloading "+fs_file.getFilename()+" "+fs_file.getUploadDate()+" "+fs_file.getMD5());
			boolean downloadReq = true;
			File file = new File(fs_file.getFilename());
			if(!file.exists()) {
				file.createNewFile();
			}else {
				String computed = FileUtils.computeMD5(file);
				if(computed.equals(fs_file.getMD5())) {
					logger.info("MD5 is equal, skipping download");
					downloadReq = false;
				}else {
					logger.info("Different md5 "+computed+" "+fs_file.getMD5());
				}
			}
			if(downloadReq) {
				try(FileOutputStream fos = new FileOutputStream(file)){
					bucket.downloadToStream(fs_file.getObjectId(), fos);
				}
			}
			
			ZipUtil.unpack(file, new File("./"));
		}
		

	}
	
	
	public static Configuration downloadConfig() throws IOException {
		URL url = new URL("config url");
		
		URLConnection con = url.openConnection();

		con.setRequestProperty("User-Agent", "PlayerRealms");
		con.setRequestProperty("API-Key", "APIKEY");
		
		YamlConfiguration config = new YamlConfiguration();
		
		try(BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream()))){
			return config.load(reader);
		}
		
	}


	
	
}
