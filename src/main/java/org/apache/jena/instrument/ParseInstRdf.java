package org.apache.jena.instrument;

/**
 * Created by rishikapoor on 11/01/2016.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hdfs.files.ReadNquadFiles;
import org.apache.jena.service.Base;
import org.apache.jena.hadoop.rdf.io.RdfIOConstants;
import org.apache.jena.hadoop.rdf.io.input.nquads.NQuadsInputFormat;
import org.apache.jena.hadoop.rdf.types.QuadWritable;

import java.security.PrivilegedAction;


/**
 * Created by rishikapoor on 18/11/2015.
 */
public class ParseInstRdf extends Base {





    public void run()

    {
        UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("platform");
        System.out.println("the user group information " + userGroupInformation.toString());
        Integer i = userGroupInformation.doAs(new PrivilegedAction<Integer>(){



            public Integer run() {


                ReadNquadFiles inoutFile = new ReadNquadFiles();
                //Configuration conf = new Configuration();
                try {
                    Configuration conf = new Configuration();
                    conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/core-site.xml").toURI()));
                    conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/hdfs-site.xml").toURI()));
                    FileSystem fs = FileSystem.get(conf);
                    Path hdfsfilePath = inoutFile.getinfile("/ingest/cmp/output/20151214-IQM-part2-rdf");
                    Path hdfsoutPath = inoutFile.writeBytes("/rishi_langRdf/");

                    if (fs.exists(hdfsoutPath)) {
                        fs.delete(hdfsoutPath, true);
                    }



                    System.out.println("INPUT FILE PATH " + hdfsfilePath);
                    System.out.println("OUTPUT FILE PATH " + hdfsoutPath);


                    Job job = new org.apache.hadoop.mapreduce.Job(conf);

                    FileInputFormat.addInputPath(job, hdfsfilePath);
                    FileOutputFormat.setOutputPath(job, hdfsoutPath);


            // This is necessary as otherwise Hadoop won't ship the JAR to all
            // nodes and you'll get ClassDefNotFound and similar errors
            job.getConfiguration().setBoolean(RdfIOConstants.INPUT_IGNORE_BAD_TUPLES, false);
            job.getConfiguration().setInt(NLineInputFormat.LINES_PER_MAP, 900000);
            job.getConfiguration().setInt(RdfIOConstants.OUTPUT_BATCH_SIZE, 25000);
            job.setJarByClass(ParseInstRdf.class);
            System.out.println("User is :" + job.getUser());

            // Give our job a friendly name
            job.setJobName("RDFparsing");
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(QuadWritable.class);

            job.setMapperClass(InstMapper.class);
            job.setReducerClass(InstReducer.class);

            job.setInputFormatClass(NQuadsInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);


            MultipleOutputs.addNamedOutput(job, "Instrument", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "InstrumentAssetClassId", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "InstrumentCommonName", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "InstrumentStatus", TextOutputFormat.class, Text.class, Text.class);


            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

                    int returnValue = job.waitForCompletion(true) ? 0 : 1;
                    fs.setPermission(hdfsoutPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));


                    System.out.println("job.isSuccessful " + job.isSuccessful());
                    System.out.println(returnValue);
                    //fs.deleteOnExit(outPut);

                    return returnValue;

                } catch (Exception ex) {

                    ex.printStackTrace();
                }
                return 0;
            }

        });

    }


}

