package org.apache.sax.instParser;

/**
 * Created by rishikapoor on 11/01/2016.
 */
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Created by rishikapoor on 08/01/2016.
 */
public class InstXmlReducer
        extends Reducer<Text, MapWritable, Text, Text> {

    private org.apache.hadoop.mapreduce.lib.output.MultipleOutputs<Text, Text> multipleOutputs;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        multipleOutputs = new org.apache.hadoop.mapreduce.lib.output.MultipleOutputs<Text, Text>(context);
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


    public void reduce(Text key, Iterable<MapWritable> values,
                       Context context)
            throws IOException, InterruptedException {
        for (MapWritable keyVal : values) {

            if (key.toString().equalsIgnoreCase("Language")) {
                outputKey.set(constructPropertyXml(key, keyVal));
                multipleOutputs.write("Instrument", null, outputKey);
            }
            else if (key.toString().equalsIgnoreCase("InstrumentAssetClassId"))
            {
                //System.out.println(keyVal);
                outputLangName.set(constructArrtibPropertyXml(key, keyVal));
                multipleOutputs.write("InstrumentAssetClassId", null, outputLangName);
            }
            else if (key.toString().equalsIgnoreCase("InstrumentCommonName"))
            {
                //System.out.println(keyVal);
                outputLangName.set(constructArrtibPropertyXml(key, keyVal));
                multipleOutputs.write("InstrumentCommonName", null, outputLangName);
            }
            else if (key.toString().equalsIgnoreCase("InstrumentStatus"))
            {
                //System.out.println(keyVal);
                outputLangName.set(constructArrtibPropertyXml(key, keyVal));
                multipleOutputs.write("InstrumentStatus", null, outputLangName);
            }
            else if (key.toString().equalsIgnoreCase("DavRcsAssetClass"))
            {
                //System.out.println(keyVal);
                outputLangName.set(constructArrtibPropertyXml(key, keyVal));
                multipleOutputs.write("DavRcsAssetClass", null, outputLangName);
            }

        }
    }

    public static String constructPropertyXml(Text key, MapWritable keyVal) throws IOException {
        StringBuilder sb = new StringBuilder();
        String InstrumentId = null;
        String CFICode = null;

        for (Writable entry : keyVal.keySet()) {

            String s = new String(WritableUtils.toByteArray(entry));
            s = s.trim();
            Writable mapValue = keyVal.get(entry);
            final Text value = (Text) mapValue;

            String k = new String(value.copyBytes(), StandardCharsets.UTF_8);
            k = k.trim();

            if (key.toString().equalsIgnoreCase("Language"))
            {
                if (s.equalsIgnoreCase("CFICode")) {
                    CFICode = k;

                } else if (s.equalsIgnoreCase("InstrumentId")) {
                    InstrumentId = k;
                }
            }

            // System.out.println(sd.toString());

        }
        sb.append(CFICode).append(';').append(InstrumentId).append(';');
        return sb.toString();
    }





    public static String constructArrtibPropertyXml(Text key,MapWritable keyVal ) throws IOException {
        StringBuilder sd = new StringBuilder();

        String InstrumentAssetClassId = null;
        String effectiveFrom = null;
        String objectTypeId= null;
        String effectiveTo= null;
        String effectiveToNACode= null;
        String effectiveFromNACode= null;
        String objectType= null;
        String InstrumentCommonName = null;
        String languageId = null;
        String InstrumentStatus = null;
        String DavRcsAssetClass = null;
        String InstrumentId = null;

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
            if(key.toString().equalsIgnoreCase("InstrumentAssetClassId"))
            {
                if(s.equalsIgnoreCase("effectiveFrom")){
                    effectiveFrom=k;

                }else if(s.equalsIgnoreCase("effectiveTo")){
                    effectiveTo=k;

                }else if(s.equalsIgnoreCase("effectiveToNACode")){
                    effectiveToNACode=k;

                }else if(s.equalsIgnoreCase("effectiveFromNACode")){
                    effectiveFromNACode=k;

                }else if(s.equalsIgnoreCase("objectTypeId")){
                    objectTypeId =k;

                }else if(s.equalsIgnoreCase("objectType")){
                    objectType =k;

                }
                else if(s.equalsIgnoreCase("InstrumentAssetClassId")){
                    InstrumentAssetClassId =k;

                }
                else if(s.equalsIgnoreCase("InstrumentId")){
                    InstrumentId =k;

                }
            }else if(key.toString().equalsIgnoreCase("InstrumentCommonName"))
            {
                if(s.equalsIgnoreCase("effectiveFrom")){
                    effectiveFrom=k;

                }else if(s.equalsIgnoreCase("effectiveTo")){
                    effectiveTo=k;

                }else if(s.equalsIgnoreCase("effectiveToNACode")){
                    effectiveToNACode=k;

                }else if(s.equalsIgnoreCase("effectiveFromNACode")){
                    effectiveFromNACode=k;

                }else if(s.equalsIgnoreCase("languageId")) {
                    languageId = k;
                }
                else if(s.equalsIgnoreCase("InstrumentCommonName")){
                    InstrumentCommonName =k;

                }
                else if(s.equalsIgnoreCase("InstrumentId")){
                    InstrumentId =k;

                }
            }else if(key.toString().equalsIgnoreCase("InstrumentStatus"))
            {
                if(s.equalsIgnoreCase("effectiveFrom")){
                    effectiveFrom=k;

                }else if(s.equalsIgnoreCase("effectiveTo")){
                    effectiveTo=k;

                }else if(s.equalsIgnoreCase("effectiveToNACode")){
                    effectiveToNACode=k;

                }else if(s.equalsIgnoreCase("effectiveFromNACode")) {
                    effectiveFromNACode = k;
                }
                else if(s.equalsIgnoreCase("InstrumentStatus")){
                    InstrumentStatus =k;

                }
                else if(s.equalsIgnoreCase("InstrumentId")){
                    InstrumentId =k;

                }
            }else if(key.toString().equalsIgnoreCase("DavRcsAssetClass"))
            {
                if(s.equalsIgnoreCase("effectiveFrom")){
                    effectiveFrom=k;

                }else if(s.equalsIgnoreCase("effectiveTo")){
                    effectiveTo=k;

                }else if(s.equalsIgnoreCase("effectiveToNACode")){
                    effectiveToNACode=k;

                }else if(s.equalsIgnoreCase("effectiveFromNACode")) {
                    effectiveFromNACode = k;
                }
                else if(s.equalsIgnoreCase("DavRcsAssetClass")){
                    DavRcsAssetClass =k;

                }
                else if(s.equalsIgnoreCase("InstrumentId")){
                    InstrumentId =k;

                }
            }
        }

        if(key.toString().equalsIgnoreCase("InstrumentCommonName")) {
            sd.append(InstrumentId).append(';').append(InstrumentCommonName).append(';').append(effectiveFrom).append(';').append(effectiveTo).append(';')
                    .append(effectiveFromNACode).append(';').append(effectiveToNACode).append(';').append(languageId);
        }else if(key.toString().equalsIgnoreCase("InstrumentAssetClassId")){
            sd.append(InstrumentId).append(';').append(InstrumentAssetClassId).append(';').append(effectiveFrom).append(';').append(effectiveTo).append(';')
                    .append(effectiveFromNACode).append(';').append(effectiveToNACode).append(';').append(objectTypeId);
        }else if(key.toString().equalsIgnoreCase("InstrumentStatus")){
            sd.append(InstrumentId).append(';').append(InstrumentStatus).append(';').append(effectiveFrom).append(';').append(effectiveTo).append(';')
                    .append(effectiveFromNACode).append(';');
        }else if(key.toString().equalsIgnoreCase("DavRcsAssetClass")){
            sd.append(InstrumentId).append(';').append(DavRcsAssetClass).append(';').append(effectiveFrom).append(';').append(effectiveTo).append(';').append(effectiveFromNACode).append(';');
        }
        // System.out.println(sd.toString());
        return sd.toString();
    }

}



