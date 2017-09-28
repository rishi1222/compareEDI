package org.apache.jena.quote;

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
import org.apache.jena.hadoop.rdf.io.input.nquads.NQuadsInputFormat;
import org.apache.jena.hadoop.rdf.types.QuadWritable;
import org.apache.jena.service.Base;

import java.security.PrivilegedAction;

/**
 * Created by rishikapoor on 07/01/2016.
 */
public class ParseQuoteRdf extends Base {





    public int run(String[] args) throws Exception {
        UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("platform");
        System.out.println("the user group information " + userGroupInformation.toString());
        Integer i = userGroupInformation.doAs(new PrivilegedAction<Integer>(){



            public Integer run() {

                Integer returnValue;
                returnValue =1;



                ReadNquadFiles inoutFile = new ReadNquadFiles();
                //Configuration conf = new Configuration();
                try {
                    Configuration conf = new Configuration();


                    conf.set("xmlinput.start","<Quote>");
                    conf.set("xmlinput.end","</Quote>");
                    conf.set("fs.defaultFS", "hdfs://Venus");
                    conf.set("dfs.nameservices","Venus");
                    conf.set("dfs.client.failover.proxy.provider.Venus","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
                    conf.set("dfs.ha.automatic-failover.enabled.Venus","true");
                    conf.set("dfs.ha.namenodes.Venus","namenode263,namenode261");
                    conf.set("dfs.namenode.rpc-address.Venus.namenode263","c149jub.int.westgroup.com:8020");
                    conf.set("dfs.namenode.servicerpc-address.Venus.namenode263","c149jub.int.westgroup.com:8022");
                    conf.set("dfs.namenode.http-address.Venus.namenode263","c149jub.int.westgroup.com:50070");
                    conf.set("dfs.namenode.https-address.Venus.namenode263","c149jub.int.westgroup.com:50470");
                    conf.set("dfs.namenode.rpc-address.Venus.namenode261","c321shu.int.westgroup.com:8020");
                    conf.set("dfs.namenode.servicerpc-address.Venus.namenode261","c321shu.int.westgroup.com:8022");
                    conf.set("dfs.namenode.http-address.Venus.namenode261","c321shu.int.westgroup.com:50070");
                    conf.set("dfs.namenode.https-address.Venus.namenode261","c321shu.int.westgroup.com:50470");
                    conf.set("mapreduce.map.output.compress","true");
                    conf.set("mapreduce.map.memory.mb","4096");
                    conf.set("mapreduce.reduce.memory.mb","4096");
                    conf.set("mapreduce.reduce.java.opts","-Djava.net.preferIPv4Stack=true -Xmx3543348019");
                    conf.set("mapreduce.map.java.opts","-Djava.net.preferIPv4Stack=true -Xmx3543348019");
                    // conf.set(HadoopIOConstants.IO_COMPRESSION_CODECS, BZip2Codec.class.getCanonicalName());
                    conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
                    conf.set("fs.hdfs.impl",
                            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
                    );
                    conf.set("fs.file.impl",
                            org.apache.hadoop.fs.LocalFileSystem.class.getName()
                    );


                    FileSystem fs = FileSystem.get(conf);

                    Path hdfsfilePath = new Path("/user/platform/ingest/cmp/output/rishi_iqm/Quotes_Full.nq");
                    Path hdfsoutPath = new Path("/user/platform/rishi_quoteRdf_new/");


                    if (fs.exists(hdfsoutPath)) {
                        fs.delete(hdfsoutPath, true);
                    }


                    Job job = new org.apache.hadoop.mapreduce.Job(conf);


                    FileInputFormat.addInputPath(job,hdfsfilePath);
                    FileOutputFormat.setOutputPath(job,hdfsoutPath);

            // This is necessary as otherwise Hadoop won't ship the JAR to all
            // nodes and you'll get ClassDefNotFound and similar errors
            //job.getConfiguration().setBoolean(RdfIOConstants.INPUT_IGNORE_BAD_TUPLES, false);
                    job.getConfiguration().setInt(NLineInputFormat.LINES_PER_MAP, 900000);
            //job.getConfiguration().setInt(RdfIOConstants.OUTPUT_BATCH_SIZE, 25000);
            job.setJarByClass(ParseQuoteRdf.class);
            System.out.println("User is :" + job.getUser());

            // Give our job a friendly name
            job.setJobName("ParseQuoteRdf");
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(QuadWritable.class);
                    job.setMapperClass(QuoteMapper.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
                    job.setReducerClass(QuoteReducer.class);


            job.setInputFormatClass(NQuadsInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);



            MultipleOutputs.addNamedOutput(job, "MIC", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "QuoteAssetClassId", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "QuoteCommonName", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "QuoteExchangeCode", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "QuoteType", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "StrikePrice", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "StrikePriceMultiplier", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "IsSuspended", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "ExpiryDate", TextOutputFormat.class, Text.class, Text.class);




                    returnValue = job.waitForCompletion(true) ? 0 : 1;
                    fs.setPermission(hdfsoutPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));


                    System.out.println("job.isSuccessful " + job.isSuccessful());
                    System.out.println(returnValue);
                    //fs.deleteOnExit(outPut);


                } catch (Exception ex) {

                    ex.printStackTrace();
                }
                return returnValue;
            }

        });

        return i;

    }

}

