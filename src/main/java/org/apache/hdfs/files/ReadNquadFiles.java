package org.apache.hdfs.files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.security.PrivilegedAction;

/**
 * Created by rishikapoor on 27/10/2015.
 */
public class ReadNquadFiles {

    public Path getHDFSinputFiles() {

        InputStreamReader source = null;
        Path hdfsfilePath = null;



        try {


            Configuration conf = new Configuration(true);

            conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/core-site.xml").toURI()));
            conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/hdfs-site.xml").toURI()));

            //conf.addResource(new Path(getClass().getClassLoader().getResource("core-site.xml").toURI()));
            //conf.addResource(new Path(getClass().getClassLoader().getResource("hdfs-site.xml").toURI()));


            //final URI uri = new URI("hdfs://c995xyn.int.westgroup.com:8020/");        //FileSystem.getDefaultUri(conf);
            //FileSystem fs = FileSystem.get(uri, conf);        //FileSystem.get(conf);
            FileSystem fs = FileSystem.get(conf);
            String hdfsinPath = fs.getUri()+"/user/platform/ingest/cmp/md_20150923_geography_nq/geography-metadata.nq";
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

    public Path writeBytes(final String fileName) throws IOException, URISyntaxException {

        UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("platform");
        System.out.println("the user group information " + userGroupInformation.toString());
        Path o = userGroupInformation.doAs(new PrivilegedAction<Path>() {


            public Path run() {

                Configuration conf = new Configuration();
                Path fos;
                fos = null;
                try {

                    conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/core-site.xml").toURI()));
                    conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/hdfs-site.xml").toURI()));

                    //uri = new URI("hdfs://c995xyn.int.westgroup.com:8020/");

                    // FileSystem fs = FileSystem.get(uri, conf);

                    FileSystem fileSystem = FileSystem.get(conf);
                    Path workingdir = fileSystem.getWorkingDirectory();
                    Path outdir = new Path(fileName);
                    fos = Path.mergePaths(workingdir , new Path(fileName));
                    //fileSystem.create(fos,false);


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


    public Path getinfile(final String fileName) throws IOException, URISyntaxException {

        UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("platform");
        System.out.println("the user group information " + userGroupInformation.toString());
        Path o = userGroupInformation.doAs(new PrivilegedAction<Path>() {


            public Path run() {

                Configuration conf = new Configuration();
                Path fos;
                fos = null;
                try {

                    conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/core-site.xml").toURI()));
                    conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/hdfs-site.xml").toURI()));

                    //uri = new URI("hdfs://c995xyn.int.westgroup.com:8020/");

                    // FileSystem fs = FileSystem.get(uri, conf);

                    FileSystem fileSystem = FileSystem.get(conf);
                    Path workingdir = fileSystem.getWorkingDirectory();
                    Path outdir = new Path(fileName);
                    fos = Path.mergePaths(workingdir , new Path(fileName));
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


    public Configuration getConfig() throws IOException, URISyntaxException {

        UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("platform");
        System.out.println("the user group information " + userGroupInformation.toString());
        Configuration o = userGroupInformation.doAs(new PrivilegedAction<Configuration>() {


            public Configuration run() {

                Configuration conf = new Configuration();

                try {

                    conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/core-site.xml").toURI()));
                    conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/hdfs-site.xml").toURI()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return conf;
            }
        });

        return o;

    }

}

