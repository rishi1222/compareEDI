package org.apache.hdfs.files;

import java.io.*;
import java.net.URISyntaxException;
import java.security.PrivilegedAction;


import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * Created by rishikapoor on 08/10/2015.
 */
public class ReadXmlFiles {


    public Path getHDFSinputFiles() {

        InputStreamReader source = null;
        Path hdfsfilePath = null;



        try {


            Configuration conf = new Configuration(true);

            conf.addResource(new Path(getClass().getClassLoader().getResource("core-site.xml").toURI()));
            conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/hdfs-site.xml").toURI()));

            //conf.addResource(new Path(getClass().getClassLoader().getResource("core-site.xml").toURI()));
            //conf.addResource(new Path(getClass().getClassLoader().getResource("hdfs-site.xml").toURI()));


            //final URI uri = new URI("hdfs://c995xyn.int.westgroup.com:8020/");        //FileSystem.getDefaultUri(conf);
            //FileSystem fs = FileSystem.get(uri, conf);        //FileSystem.get(conf);
            FileSystem fs = FileSystem.get(conf);
            String hdfsinPath = fs.getUri()+"/user/bolddemo/ftpsdi/contentmarket/MetadataManagement/raw/MDM.Language.All.1.2015-09-01-0000.Full.ZIP.MDM.Language.All.1.2015-09-01-0000.Full.xml";
            hdfsfilePath = new Path(hdfsinPath);

            System.out.println("Filesystem URI : " + fs.getUri());
            System.out.println("Filesystem Home Directory : " + fs.getHomeDirectory());
            System.out.println("Filesystem Working Directory : " + fs.getWorkingDirectory());
            System.out.println("HDFS File Path : " + hdfsfilePath);



            source = new InputStreamReader(fs.open(hdfsfilePath));

        } catch (Exception e) {
            e.printStackTrace();
        }

        return hdfsfilePath;
    }
    public Path getinfile(final String fileName) throws IOException, URISyntaxException {

        UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("platform");
        System.out.println("the user group information " + userGroupInformation.toString());
        Path o = userGroupInformation.doAs(new PrivilegedAction<Path>() {


            public Path run() {

                Configuration conf = new Configuration();
                Path fos;
                fos = null;
                try {

                    conf.addResource(new Path(getClass().getClassLoader().getResource("core-site.xml").toURI()));
                    conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/hdfs-site.xml").toURI()));

                    //uri = new URI("hdfs://c995xyn.int.westgroup.com:8020/");

                    // FileSystem fs = FileSystem.get(uri, conf);

                    FileSystem fileSystem = FileSystem.get(conf);
                    Path workingdir = fileSystem.getWorkingDirectory();
                    Path outdir = new Path(fileName);
                    fos = new Path(fileName);
                    String outFile = fileSystem.getUri() +fileName;

                    //fos = new Path(outFile);

                    System.out.println("Filesystem URI : " + fileSystem.getUri());
                    System.out.println("Filesystem Home Directory : " + fileSystem.getHomeDirectory());
                    System.out.println("Filesystem Working Directory : " + fileSystem.getWorkingDirectory());
                    System.out.println("HDFS File Path : " + fos);

                } catch (Exception e) {
                    e.printStackTrace();
                }
                return fos;
            }
        });

        return o;

    }

    public Path writeBytes(final String fileName) throws IOException, URISyntaxException {

        UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("platform");
        Path o = userGroupInformation.doAs(new PrivilegedAction<Path>() {


            public Path run() {

                Configuration conf = new Configuration();
                Path fos;
                fos = null;
                try {

                    conf.addResource(new Path(getClass().getClassLoader().getResource("core-site.xml").toURI()));
                    conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/hdfs-site.xml").toURI()));

                    //uri = new URI("hdfs://c995xyn.int.westgroup.com:8020/");

                    // FileSystem fs = FileSystem.get(uri, conf);

                    FileSystem fileSystem = FileSystem.get(conf);
                    Path workingdir = fileSystem.getWorkingDirectory();
                    Path path = new Path(fileName);
                    fos = Path.mergePaths(workingdir , path);

                } catch (Exception e) {
                    e.printStackTrace();
                }
                return fos;
            }
        });

        return o;

    }
}
