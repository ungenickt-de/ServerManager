package com.playerrealms.servermanager.tool;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.digest.DigestUtils;

public class FileUtils {

	public static String computeMD5(File file) throws IOException {
		try(FileInputStream fis = new FileInputStream(file)){
			return DigestUtils.md5Hex(fis);
		}
	}
	
}
