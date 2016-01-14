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
import org.apache.sax.instParser.InstXmlMapper;
import org.apache.sax.instParser.InstXmlReducer;
import org.apache.sax.parser.Reduce;
import org.apache.sax.parser.XmlInputFormat;
import org.apache.sax.parser.XmlParsingMap;

import java.security.PrivilegedAction;

/**
 * Created by rishikapoor on 11/01/2016.
 */
public class ParseQuoteXml extends Base {



    public void run()

    {
        UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("platform");
        System.out.println("the user group information " + userGroupInformation.toString());
        Integer i = userGroupInformation.doAs(new PrivilegedAction<Integer>(){



            public Integer run() {

                try {


                    Configuration conf = new Configuration();
                    ReadXmlFiles inoutPath = new ReadXmlFiles();
                    Path xmlInputPath = inoutPath.getinfile("/user/boldqed/content/IQM/Full/EquitiesDerivativesAndFundsQuote.Quote_Full/raw/");

                    Path xmlOutPutPath = inoutPath.writeBytes("/rishi_langXml_Quote/");


                    conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/core-site.xml").toURI()));
                    conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/hdfs-site.xml").toURI()));
                    conf.addResource(new Path(getClass().getClassLoader().getResource("./HDFS_CONFIG/mapred-site.xml").toURI()));
                    conf.set("xmlinput.start","<Quote>");
                    conf.set("xmlinput.end","</Quote>");

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
