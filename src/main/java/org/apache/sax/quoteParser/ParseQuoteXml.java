package org.apache.sax.quoteParser;

/**
 * Created by rishikapoor on 13/01/2016.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hdfs.files.ReadXmlFiles;
import org.apache.jena.service.Base;

import java.security.PrivilegedAction;

/**
 * Created by rishikapoor on 11/01/2016.
 */
public class ParseQuoteXml extends Base {



    public int run(String[] args) throws Exception

    {
        UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("platform");
        System.out.println("the user group information " + userGroupInformation.toString());
        Integer i = userGroupInformation.doAs(new PrivilegedAction<Integer>(){


            public Integer run() {

                Integer returnValue;
                returnValue =1;

                try {


                    Configuration conf = new Configuration();

                    ReadXmlFiles inoutPath = new ReadXmlFiles();
//                    Path xmlInputPath = inoutPath.getinfile("hdfs://Venus/user/boldqed/content/IQM/Full/EquitiesDerivativesAndFundsQuote.Quote_Full/raw/");

//                    Path xmlOutPutPath = inoutPath.writeBytes("hdfs://Venus/user/platform/rishi_langXml_Quote/");


//
//                    conf.addResource(new Path("core-site.xml"));
//                    conf.addResource(new Path("hdfs-site.xml"));
//                    conf.addResource(new Path("mapred-site.xml"));
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
                    conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
                    conf.set("mapred.local.dir","tmp_output");

                    conf.set("fs.hdfs.impl",
                            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
                    );
                    conf.set("fs.file.impl",
                            org.apache.hadoop.fs.LocalFileSystem.class.getName()
                    );

                    FileSystem fs = FileSystem.get(conf);
                    Path xmlInputPath = new Path("/user/boldqed/content/IQM/Full/EquitiesDerivativesAndFundsQuote.Quote_Full/raw/");
                    Path xmlOutPutPath = new Path("/user/platform/rishi_langXml_Quote/");
                    if (fs.exists(xmlOutPutPath)) {
                        fs.delete(xmlOutPutPath, true);
                    }





                    Job job = new org.apache.hadoop.mapreduce.Job(conf);

                    RemoteIterator<LocatedFileStatus> ri =  fs.listFiles(xmlInputPath, true);
                    while (ri.hasNext()) {
                        LocatedFileStatus fileStatus = ri.next();

                        if(fileStatus.getPath().toString().contains("2015-10-31")) {
                            System.out.println("chmod a+rwx on {}" + fileStatus.getPath().toString());
                            FileInputFormat.addInputPaths(job, fileStatus.getPath().toString());
                        }


                    }


                    FileOutputFormat.setOutputPath(job, xmlOutPutPath);



                    job.setJobName("ParseQuoteXml");

                    job.setJarByClass(ParseQuoteXml.class);




                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(MapWritable.class);

                    job.setMapperClass(QuoteXmlMapper.class);
                    job.setReducerClass(QuoteXmlReducer.class);

                    job.setInputFormatClass(org.apache.sax.parser.XmlInputFormat.class);
                    job.setOutputFormatClass(TextOutputFormat.class);
                    MultipleOutputs.addNamedOutput(job, "Quotes", TextOutputFormat.class, Text.class, MapWritable.class);
                    MultipleOutputs.addNamedOutput(job, "QuoteAssetClassId", TextOutputFormat.class, Text.class, MapWritable.class);
                    MultipleOutputs.addNamedOutput(job, "QuoteCommonName", TextOutputFormat.class, Text.class, MapWritable.class);
                    MultipleOutputs.addNamedOutput(job, "MIC", TextOutputFormat.class, Text.class, MapWritable.class);
                    MultipleOutputs.addNamedOutput(job, "StrikePrice", TextOutputFormat.class, Text.class, MapWritable.class);
                    MultipleOutputs.addNamedOutput(job, "StrikePriceMultiplier", TextOutputFormat.class, Text.class, MapWritable.class);
                    MultipleOutputs.addNamedOutput(job, "ExpiryDate", TextOutputFormat.class, Text.class, MapWritable.class);
                    MultipleOutputs.addNamedOutput(job, "QuoteIsActive", TextOutputFormat.class, Text.class, MapWritable.class);
                    MultipleOutputs.addNamedOutput(job, "IsSuspended", TextOutputFormat.class, Text.class, MapWritable.class);
                    MultipleOutputs.addNamedOutput(job, "QuoteType", TextOutputFormat.class, Text.class, MapWritable.class);
                    MultipleOutputs.addNamedOutput(job, "QuoteExchangeCode", TextOutputFormat.class, Text.class, MapWritable.class);




                    returnValue = job.waitForCompletion(true) ? 0:1;
                    System.out.println("job.isSuccessful " + job.isSuccessful());
                    System.out.println(returnValue);



                } catch (Exception ex) {

                    ex.printStackTrace();
                }
                return returnValue;
            }

        });

        return i;

    }



}
