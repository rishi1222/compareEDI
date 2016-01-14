package org.apache.sax.quoteParser;

/**
 * Created by rishikapoor on 13/01/2016.
 */
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Created by rishikapoor on 11/11/2015.
 */
public class QuoteXmlReducer
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
                multipleOutputs.write("Quotes", null, outputKey);
            }
            else if (key.toString().equalsIgnoreCase("QuoteAssetClassId"))
            {
                //System.out.println(keyVal);
                outputLangName.set(constructArrtibPropertyXml(key, keyVal));
                multipleOutputs.write("QuoteAssetClassId", null, outputLangName);
            }
            else if (key.toString().equalsIgnoreCase("QuoteCommonName"))
            {
                //System.out.println(keyVal);
                outputLangName.set(constructArrtibPropertyXml(key, keyVal));
                multipleOutputs.write("QuoteCommonName", null, outputLangName);
            }
            else if (key.toString().equalsIgnoreCase("MIC"))
            {
                //System.out.println(keyVal);
                outputLangName.set(constructArrtibPropertyXml(key, keyVal));
                multipleOutputs.write("MIC", null, outputLangName);
            }
            else if (key.toString().equalsIgnoreCase("QuoteExchangeCode"))
            {
                //System.out.println(keyVal);
                outputLangName.set(constructArrtibPropertyXml(key, keyVal));
                multipleOutputs.write("QuoteExchangeCode", null, outputLangName);
            }
            else if (key.toString().equalsIgnoreCase("StrikePrice"))
            {
                //System.out.println(keyVal);
                outputLangName.set(constructArrtibPropertyXml(key, keyVal));
                multipleOutputs.write("StrikePrice", null, outputLangName);
            }
            else if (key.toString().equalsIgnoreCase("StrikePriceMultiplier"))
            {
                //System.out.println(keyVal);
                outputLangName.set(constructArrtibPropertyXml(key, keyVal));
                multipleOutputs.write("StrikePriceMultiplier", null, outputLangName);
            }
            else if (key.toString().equalsIgnoreCase("ExpiryDate"))
            {
                //System.out.println(keyVal);
                outputLangName.set(constructArrtibPropertyXml(key, keyVal));
                multipleOutputs.write("ExpiryDate", null, outputLangName);
            }
            else if (key.toString().equalsIgnoreCase("IsSuspended"))
            {
                //System.out.println(keyVal);
                outputLangName.set(constructArrtibPropertyXml(key, keyVal));
                multipleOutputs.write("IsSuspended", null, outputLangName);
            }
            else if (key.toString().equalsIgnoreCase("QuoteType"))
            {
                //System.out.println(keyVal);
                outputLangName.set(constructArrtibPropertyXml(key, keyVal));
                multipleOutputs.write("QuoteType", null, outputLangName);
            }

        }
    }

    public static String constructPropertyXml(Text key, MapWritable keyVal) throws IOException {
        StringBuilder sb = new StringBuilder();
        String CallPutOption = null;
        String QuoteId = null;

        for (Writable entry : keyVal.keySet()) {

            String s = new String(WritableUtils.toByteArray(entry));
            s = s.trim();
            Writable mapValue = keyVal.get(entry);
            final Text value = (Text) mapValue;

            String k = new String(value.copyBytes(), StandardCharsets.UTF_8);
            k = k.trim();

            if (key.toString().equalsIgnoreCase("Language"))
            {
                if (s.equalsIgnoreCase("CallPutOption")) {
                    CallPutOption = k;

                } else if (s.equalsIgnoreCase("QuoteId")) {
                    QuoteId = k;
                }
            }

            // System.out.println(sd.toString());

        }
        sb.append(CallPutOption).append(';').append(QuoteId).append(';');
        return sb.toString();
    }





    public static String constructArrtibPropertyXml(Text key,MapWritable keyVal ) throws IOException {
        StringBuilder sd = new StringBuilder();

        String QuoteAssetClassId = null;
        String effectiveFrom = null;
        String objectTypeId= null;
        String effectiveTo= null;
        String effectiveToNACode= null;
        String effectiveFromNACode= null;
        String objectType= null;
        String QuoteCommonName = null;
        String languageId = null;
        String MIC = null;
        String QuoteExchangeCode = null;
        String StrikePrice = null;
        String StrikePriceMultiplier = null;
        String ExpiryDate = null;
        String QuoteIsActive = null;
        String IsSuspended = null;
        String QuoteType = null;
        String CallPutOption = null;
        String QuoteId = null;

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
            if(key.toString().equalsIgnoreCase("QuoteAssetClassId"))
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
                else if(s.equalsIgnoreCase("QuoteAssetClassId")){
                    QuoteAssetClassId =k;

                }
                else if(s.equalsIgnoreCase("QuoteId")){
                    QuoteId =k;

                }
            }else if(key.toString().equalsIgnoreCase("QuoteCommonName"))
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
                else if(s.equalsIgnoreCase("QuoteCommonName")){
                    QuoteCommonName =k;

                }
                else if(s.equalsIgnoreCase("QuoteId")){
                    QuoteId =k;

                }
            }else if(key.toString().equalsIgnoreCase("MIC"))
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
                else if(s.equalsIgnoreCase("MIC")){
                    MIC =k;

                }
                else if(s.equalsIgnoreCase("QuoteId")){
                    QuoteId =k;

                }
            }else if(key.toString().equalsIgnoreCase("QuoteExchangeCode"))
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
                else if(s.equalsIgnoreCase("QuoteExchangeCode")){
                    QuoteExchangeCode =k;

                }
                else if(s.equalsIgnoreCase("QuoteId")){
                    QuoteId =k;

                }
            }
            else if(key.toString().equalsIgnoreCase("StrikePrice"))
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
                else if(s.equalsIgnoreCase("StrikePrice")){
                    StrikePrice =k;

                }
                else if(s.equalsIgnoreCase("QuoteId")){
                    QuoteId =k;

                }
            }
            else if(key.toString().equalsIgnoreCase("StrikePriceMultiplier"))
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
                else if(s.equalsIgnoreCase("StrikePriceMultiplier")){
                    StrikePriceMultiplier =k;

                }
                else if(s.equalsIgnoreCase("QuoteId")){
                    QuoteId =k;

                }
            }
            else if(key.toString().equalsIgnoreCase("ExpiryDate"))
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
                else if(s.equalsIgnoreCase("ExpiryDate")){
                    ExpiryDate =k;

                }
                else if(s.equalsIgnoreCase("QuoteId")){
                    QuoteId =k;

                }
            }
            else if(key.toString().equalsIgnoreCase("QuoteIsActive"))
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
                else if(s.equalsIgnoreCase("QuoteIsActive")){
                    QuoteIsActive =k;

                }
                else if(s.equalsIgnoreCase("QuoteId")){
                    QuoteId =k;

                }
            }
            else if(key.toString().equalsIgnoreCase("IsSuspended"))
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
                else if(s.equalsIgnoreCase("IsSuspended")){
                    IsSuspended =k;

                }
                else if(s.equalsIgnoreCase("QuoteId")){
                    QuoteId =k;

                }
            }
            else if(key.toString().equalsIgnoreCase("QuoteType"))
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
                else if(s.equalsIgnoreCase("QuoteType")){
                    QuoteType =k;

                }
                else if(s.equalsIgnoreCase("QuoteId")){
                    QuoteId =k;

                }
            }

        }

        if(key.toString().equalsIgnoreCase("QuoteCommonName")) {
            sd.append(QuoteId).append(';').append(QuoteCommonName).append(';').append(effectiveFrom).append(';').append(effectiveTo).append(';').append(effectiveFromNACode).append(';').append(effectiveToNACode).append(';').append(languageId);
        }else if(key.toString().equalsIgnoreCase("QuoteAssetClassId")){
            sd.append(QuoteId).append(';').append(QuoteAssetClassId).append(';').append(effectiveFrom).append(';').append(effectiveTo).append(';').append(effectiveFromNACode).append(';').append(effectiveToNACode).append(';').append(objectTypeId);
        }else if(key.toString().equalsIgnoreCase("MIC")){
            sd.append(QuoteId).append(';').append(MIC).append(';').append(effectiveFrom).append(';').append(effectiveTo).append(';').append(effectiveFromNACode).append(';').append(effectiveToNACode);
        }else if(key.toString().equalsIgnoreCase("QuoteExchangeCode")){
            sd.append(QuoteId).append(';').append(QuoteExchangeCode).append(';').append(effectiveFrom).append(';').append(effectiveTo).append(';').append(effectiveFromNACode).append(';').append(effectiveToNACode);
        }else if(key.toString().equalsIgnoreCase("StrikePrice")){
            sd.append(QuoteId).append(';').append(StrikePrice).append(';').append(effectiveFrom).append(';').append(effectiveTo).append(';').append(effectiveFromNACode).append(';').append(effectiveToNACode);
        }else if(key.toString().equalsIgnoreCase("StrikePriceMultiplier")){
            sd.append(QuoteId).append(';').append(StrikePriceMultiplier).append(';').append(effectiveFrom).append(';').append(effectiveTo).append(';').append(effectiveFromNACode).append(';').append(effectiveToNACode);
        }else if(key.toString().equalsIgnoreCase("ExpiryDate")){
            sd.append(QuoteId).append(';').append(ExpiryDate).append(';').append(effectiveFrom).append(';').append(effectiveTo).append(';').append(effectiveFromNACode).append(';').append(effectiveToNACode);
        }else if(key.toString().equalsIgnoreCase("QuoteIsActive")){
            sd.append(QuoteId).append(';').append(QuoteIsActive).append(';').append(effectiveFrom).append(';').append(effectiveTo).append(';').append(effectiveFromNACode).append(';').append(effectiveToNACode);
        }else if(key.toString().equalsIgnoreCase("IsSuspended")){
            sd.append(QuoteId).append(';').append(IsSuspended).append(';').append(effectiveFrom).append(';').append(effectiveTo).append(';').append(effectiveFromNACode).append(';').append(effectiveToNACode);
        } else if(key.toString().equalsIgnoreCase("QuoteType")){
            sd.append(QuoteId).append(';').append(QuoteType).append(';').append(effectiveFrom).append(';').append(effectiveTo).append(';').append(effectiveFromNACode).append(';').append(effectiveToNACode);
        }
        // System.out.println(sd.toString());
        return sd.toString();
    }

}


