package org.apache.sax.instParser;

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
import org.apache.sax.parser.Reduce;
import org.apache.sax.parser.XmlInputFormat;
import org.apache.sax.parser.XmlParsingMap;

import java.security.PrivilegedAction;

/**
 * Created by rishikapoor on 11/01/2016.
 */
public class ParseInstXml extends Base {



    public void run()

    {
        UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("platform");
        System.out.println("the user group information " + userGroupInformation.toString());
        Integer i = userGroupInformation.doAs(new PrivilegedAction<Integer>(){



            public Integer run() {

                try {


                    Configuration conf = new Configuration();
                    ReadXmlFiles inoutPath = new ReadXmlFiles();
                    Path xmlInputPath = inoutPath.getinfile("/user/boldqed/content/IQM/Full/EquitiesDerivativesAndFundsQuote.Instrument_Full/raw/");

                    Path xmlOutPutPath = inoutPath.writeBytes("/rishi_langXml/");


                    conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/core-site.xml").toURI()));
                    conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/hdfs-site.xml").toURI()));
                    conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/mapred-site.xml").toURI()));
                    conf.set("xmlinput.start","<Instrument>");
                    conf.set("xmlinput.end","</Instrument>");

                    FileSystem fs = FileSystem.get(conf);
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



                    job.setJobName("ParseInstXml");

                    job.setJarByClass(ParseInstXml.class);




                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(MapWritable.class);

                    job.setMapperClass(InstXmlMapper.class);
                    job.setReducerClass(InstXmlReducer.class);

                    job.setInputFormatClass(XmlInputFormat.class);
                    job.setOutputFormatClass(TextOutputFormat.class);
                    MultipleOutputs.addNamedOutput(job, "Instrument", TextOutputFormat.class, Text.class, MapWritable.class);
                    MultipleOutputs.addNamedOutput(job, "InstrumentAssetClassId", TextOutputFormat.class, Text.class, MapWritable.class);
                    MultipleOutputs.addNamedOutput(job, "InstrumentCommonName", TextOutputFormat.class, Text.class, MapWritable.class);
                    MultipleOutputs.addNamedOutput(job, "InstrumentStatus", TextOutputFormat.class, Text.class, MapWritable.class);
                    MultipleOutputs.addNamedOutput(job, "DavRcsAssetClass", TextOutputFormat.class, Text.class, MapWritable.class);




                    int returnValue = job.waitForCompletion(true) ? 0:1;
                    System.out.println("job.isSuccessful " + job.isSuccessful());
                    System.out.println(returnValue);


                    return returnValue;

                } catch (Exception ex) {

                    ex.printStackTrace();
                }
                return 0;
            }

        });

    }




}