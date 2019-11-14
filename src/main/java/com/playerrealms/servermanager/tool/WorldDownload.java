package com.playerrealms.servermanager.tool;

import org.apache.commons.io.FileUtils;
import org.zeroturnaround.zip.ZipUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.logging.Logger;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

public class WorldDownload {

    public static final long MAX_DOWNLOAD_SIZE = 52_428_800;

    public static void downloadWorld(URL website, File destinationFolder, Logger logger) throws IOException {
        long time = System.currentTimeMillis();
        ReadableByteChannel rbc = Channels.newChannel(website.openStream());

        File worldFile = new File(destinationFolder, "world.zip");
        try (FileOutputStream fos = new FileOutputStream(worldFile)) {
            fos.getChannel().transferFrom(rbc, 0, MAX_DOWNLOAD_SIZE + 64);
        }
        if (worldFile.length() > MAX_DOWNLOAD_SIZE) {
            worldFile.delete();
            logger.warning("Downloaded world but it was too large!!");
            return;
        }
        logger.info("Finished download in " + (System.currentTimeMillis() - time) + " ms");
        time = System.currentTimeMillis();
        File uploadFolder = new File(destinationFolder, "upload");
        uploadFolder.mkdir();
        ZipUtil.unpack(worldFile, uploadFolder);
        checkSafety(uploadFolder);
        File worldFolder = findWorldFolder(uploadFolder);
        for (File srcFile : worldFolder.listFiles()) {
            if (srcFile.isDirectory()) {
                FileUtils.copyDirectoryToDirectory(srcFile, destinationFolder);
            } else {
                FileUtils.copyFileToDirectory(srcFile, destinationFolder);
            }
        }
        worldFile.delete();
        FileUtils.deleteDirectory(uploadFolder);
        logger.info("Finished world extract in " + (System.currentTimeMillis() - time) + " ms");

    }

    private static File findWorldFolder(File folder) {
        if (!folder.isDirectory()) {
            if (folder.getName().equals("level.dat")) {
                return folder.getParentFile();
            }
            return null;
        }
        for (File file : folder.listFiles()) {
            File found = findWorldFolder(file);

            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static void checkSafety(File folder) throws IOException, ZipException {
        for (File file : folder.listFiles()) {
            if (file.getName().equalsIgnoreCase("plugins") && file.isDirectory()) {
                FileUtils.deleteDirectory(file);
            } else if (file.getName().endsWith(".jar")) {
                FileUtils.forceDelete(file);
            } else if (file.getName().endsWith(".properties") || file.getName().endsWith(".yml")) {
                FileUtils.forceDelete(file);
            } else if (file.getName().equalsIgnoreCase("players")) {
                FileUtils.deleteDirectory(file);
            } else if (isZipFile(file)) {
                FileUtils.forceDelete(file);
            } else if (file.isDirectory()) {
                checkSafety(file);
            }
        }
    }

    private static boolean isZipFile(File f) {
        if (f.isDirectory()) {
            return false;
        }
        try {
            ZipFile file = new ZipFile(f);
            return true;
        } catch (ZipException e) {
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}
