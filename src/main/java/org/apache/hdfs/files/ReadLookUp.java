package org.apache.hdfs.files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.security.PrivilegedAction;

/**
 * Created by rishikapoor on 09/11/2015.
 */
public class ReadLookUp {
    public InputStream getHDFSinputFiles() {

        InputStream source = null;
        Path hdfsfilePath = null;


        String hdfsinPath = "/user/bolddemo/ftpsdi/contentmarket/MetadataManagement/raw/MDM.Identifiers.All.3.2015-09-01-0000.Full.ZIP.MDM.Identifiers.All.3.2015-09-01-0000.Full.xml";
        try {


            Configuration conf = new Configuration(true);

            conf.addResource(new Path(getClass().getClassLoader().getResource("core-site.xml").toURI()));
            conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/hdfs-site.xml").toURI()));

            //conf.addResource(new Path(getClass().getClassLoader().getResource("core-site.xml").toURI()));
            //conf.addResource(new Path(getClass().getClassLoader().getResource("hdfs-site.xml").toURI()));


            //final URI uri = new URI("hdfs://c995xyn.int.westgroup.com:8020/");        //FileSystem.getDefaultUri(conf);
            //FileSystem fs = FileSystem.get(uri, conf);        //FileSystem.get(conf);
            FileSystem fs = FileSystem.get(conf);
            hdfsfilePath = new Path(hdfsinPath);

            System.out.println("Filesystem URI : " + fs.getUri());
            System.out.println("Filesystem Home Directory : " + fs.getHomeDirectory());
            System.out.println("Filesystem Working Directory : " + fs.getWorkingDirectory());
            System.out.println("HDFS File Path : " + hdfsfilePath);



            source = fs.open(hdfsfilePath);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return source;
    }


    public OutputStream writeBytes(final String fileName) throws IOException, URISyntaxException {

        UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("platform");
        OutputStream o = userGroupInformation.doAs(new PrivilegedAction<OutputStream>() {


            public OutputStream run() {

                Configuration conf = new Configuration();
                OutputStream fos;
                fos = null;
                try {

                    conf.addResource(new Path(getClass().getClassLoader().getResource("core-site.xml").toURI()));
                    conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/hdfs-site.xml").toURI()));

                    //uri = new URI("hdfs://c995xyn.int.westgroup.com:8020/");

                    // FileSystem fs = FileSystem.get(uri, conf);

                    FileSystem fileSystem = FileSystem.get(conf);
                    Path path = new Path(fileName);
                    fos = fileSystem.create(path);

                } catch (Exception e) {
                    e.printStackTrace();
                }
                return fos;
            }
        });

        return o;

    }

}
