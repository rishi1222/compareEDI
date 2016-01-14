package org.apache.sax.parser;


import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by rishikapoor on 11/11/2015.
 */
public class Reduce
        extends Reducer<Text, MapWritable, Text, Text> {

    private org.apache.hadoop.mapreduce.lib.output.MultipleOutputs<Text, Text> multipleOutputs;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
            multipleOutputs = new org.apache.hadoop.mapreduce.lib.output.MultipleOutputs<Text,Text>(context);
        //context.write(new Text("<configuration>"), null);
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        //context.write(new Text("</configuration>"), null);
            multipleOutputs.close();
    }

    private Text outputKey = new Text();
    private Text outputLangName = new Text();



    public  void reduce(Text key, Iterable<MapWritable> values,
                      Context context)
            throws IOException, InterruptedException {
        for (MapWritable keyVal : values) {

            if(key.toString().equalsIgnoreCase("Language")) {
                outputKey.set(constructPropertyXml(key, keyVal));
                //System.out.println(keyVal);
                //multipleOutputs.write("Lang", null, outputKey);
            }else if (key.toString().equalsIgnoreCase("LanguageName"))
            {
                //System.out.println(keyVal);
                outputLangName.set(constructArrtibPropertyXml(key, keyVal));
                multipleOutputs.write("LangName", null, outputLangName);
            }

            }
        }


    public static String constructPropertyXml(Text key,MapWritable keyVal ) throws IOException {
        StringBuilder sb = new StringBuilder();
        String LanguageGeographyId = null;
        String activeFrom = null;
        String LanguageId = null;
        String BaseLanguageId = null;
        String LanguageUniqueName = null;
        String LanguageScriptId = null;

        for (Writable entry : keyVal.keySet())
        {

            String s = new String(WritableUtils.toByteArray(entry));
            s = s.trim();
            Writable mapValue = keyVal.get(entry);
            final Text value = (Text)mapValue;

            String k = new String(value.copyBytes(), StandardCharsets.UTF_8);
            //System.out.println(k);
            k = k.trim();
            if (key.toString().equalsIgnoreCase("Language"))
            {
                //System.out.println("Value of s is :" + s);
                if (s.equalsIgnoreCase("LanguageGeographyId")) {
                    LanguageGeographyId = k;
                } else if (s.equalsIgnoreCase("activeFrom")) {
                    activeFrom = k;
                } else if (s.equalsIgnoreCase("LanguageId")) {
                    LanguageId = k;
                } else if (s.equalsIgnoreCase("BaseLanguageId")) {
                    BaseLanguageId = k;
                } else if (s.equalsIgnoreCase("LanguageUniqueName")) {
                    LanguageUniqueName = k;
                } else if (s.equalsIgnoreCase("LanguageScriptId")) {
                    LanguageScriptId = k;
                }
            }
        }

            sb.append(LanguageId).append(';').append(BaseLanguageId).append(';').append(LanguageUniqueName).append(';').append(LanguageScriptId).append(';').append(LanguageGeographyId).append(';').append(activeFrom);
            //System.out.println(sb.toString());
            return sb.toString();
        }


    public static String constructArrtibPropertyXml(Text key,MapWritable keyVal ) throws IOException {
        StringBuilder sd = new StringBuilder();

        String langname = null;
        String effectiveFrom = null;
        String effectiveTo= null;
        String effectiveToNACode= null;
        String IdentifierValue= null;
        String identifierEntityId= null;
        String identifierEntityTypeId= null;

        //System.out.println("The value of context key is :" + key);
        for(Writable entry : keyVal.keySet()) {

            String s = new String(WritableUtils.toByteArray(entry));

            //System.out.println(s);
            s = s.trim();
            Writable mapValue = keyVal.get(entry);

            final Text value = (Text)mapValue;

            String k = new String(value.copyBytes(), StandardCharsets.UTF_8);
//            final ByteArrayOutputStream out = new ByteArrayOutputStream();
//            final DataOutputStream dos = new DataOutputStream(out);
//            mapValue.write(dos);
//            System.out.println(new  String(out.toByteArray(), StandardCharsets.UTF_8));
//            System.out.println(k);
            k = k.trim();
            if(key.toString().equalsIgnoreCase("LanguageName"))
            {
                if(s.equalsIgnoreCase("effectiveFrom")){
                    effectiveFrom=k;

                }else if(s.equalsIgnoreCase("effectiveTo")){
                    effectiveTo=k;

                }else if(s.equalsIgnoreCase("effectiveToNACode")){
                    effectiveToNACode=k;

                }else if(s.equalsIgnoreCase("identifierEntityId")){
                    identifierEntityId=k;

                }else if(s.equalsIgnoreCase("identifierEntityTypeId")){
                    identifierEntityTypeId =k;

                }else if(s.equalsIgnoreCase("IdentifierValue")){
                    IdentifierValue =k;

                }
            }
        }
        sd.append(effectiveFrom).append(';').append(effectiveTo).append(';').append(effectiveToNACode).append(';').append(identifierEntityId).append(';').append(identifierEntityTypeId).
                append(';').append(IdentifierValue);
       // System.out.println(sd.toString());
        return sd.toString();
    }
}

