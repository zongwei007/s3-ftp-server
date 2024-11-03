package com.s3.ftp;

import com.s3.ftp.config.GlobalConfiguration;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;

import java.nio.file.Path;

public final class S3FTPServer {

    public static void main(String[] args) throws Exception {
        GlobalConfiguration.load(Path.of(args.length > 0 ? args[0] : "config.properties"));

        FtpServerFactory serverFactory = GlobalConfiguration.createServerFactory();

        FtpServer server = serverFactory.createServer();
        server.start();
    }

}
