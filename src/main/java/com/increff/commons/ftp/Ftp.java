/*
 * Copyright (c) 2021. Increff
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.increff.commons.ftp;

import org.apache.commons.net.ftp.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Ftp {

    private FTPClient ftpClient;

    public void login(String username, String password, String host, int timeout) throws IOException {
        ftpClient = new FTPClient();
        ftpClient.setConnectTimeout(timeout);
        ftpClient.connect(host);
        ftpClient.enterLocalPassiveMode();
        ftpClient.login(username, password);
        if (!FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {
            ftpClient.disconnect();
            throw new FTPConnectionClosedException("Unable to connect to FTP server...");
        }

    }

    public void logout() throws IOException {
        ftpClient.logout();
        ftpClient.disconnect();
    }

    public boolean downloadRemoteFile(String remoteFilePath, String savePath) throws IOException {
        File downloadFile = new File(savePath);
        OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(downloadFile));
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
        boolean success = ftpClient.retrieveFile(remoteFilePath, outputStream);
        if (outputStream != null) {
            outputStream.close();
        }
        return success;
    }

    public void downloadRemoteDirectory(String remotePath, String localPath) throws IOException, FtpException {
        List<FTPFile> fileList = Arrays.asList(ftpClient.listFiles(remotePath)).stream().filter(x -> !(x.getName().equals(".") || x.getName().equals(".."))).collect(Collectors.toList());
        if (fileList.isEmpty()) {
            return;
        }
        for (FTPFile aFile : fileList) {
            String currentFileName = aFile.getName();
            String filePath = remotePath + currentFileName;
            String newDirPath = localPath + currentFileName;
            if (!aFile.isDirectory()) {
                downloadRemoteFile(filePath, newDirPath);
            }
        }
    }

    public List<String> getRemoteFileNames(String remoteDir) throws IOException {
        List<FTPFile> subFiles = Arrays.asList(ftpClient.listFiles(remoteDir));
        List<String> fileNames = new ArrayList<>();
        for (FTPFile file : subFiles) {
            if (!file.isDirectory())
                fileNames.add(file.getName());
        }
        return fileNames;
    }

    public void putRemoteFile(String remoteDir, String localPath) throws IOException, FtpException {
        File file = new File(localPath);
        FileInputStream fis = new FileInputStream(file);
        boolean success = ftpClient.storeFile(remoteDir, fis);
        if (!success) {
            throw new FtpException("cannot upload file");
        }
    }

    public void deleteRemoteFile(String remotePath) throws IOException, FtpException {
        boolean exist = ftpClient.deleteFile(remotePath);
        if (!exist) {
            throw new FtpException("file not found");
        }
    }

    public void deleteLocalFile(String path) throws FtpException {
        File file = new File(path);
        boolean exist = file.delete();
        if (!exist) {
            throw new FtpException("FIle not found");
        }
    }

    public void renameRemoteFile(String oldPath, String newPath) throws IOException, FtpException {
        boolean success = ftpClient.rename(oldPath, newPath);
        if (!success) {
            throw new FtpException("Cannot rename file");
        }
    }

    public void renameLocalFile(String oldPath, String newPath) throws FtpException {
        File oldFile = new File(oldPath);
        File newFile = new File(newPath);
        boolean success = oldFile.renameTo(newFile);
        if (!success) {
            throw new FtpException("Cannot rename file");
        }
    }

    public void moveLocalFile(String oldPath, String newPath) throws IOException, FtpException {
        Path temp = Files.move(Paths.get(oldPath), Paths.get(newPath));
        if (temp == null) {
            throw new FtpException("Cannot move file");
        }
    }

    private int getTimeout(int ftpTimeout) {
        int max = 60 * 60 * 1000; //1 hour
        return Math.min(ftpTimeout, max);
    }


}
