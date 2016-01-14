package org.apache.sax.parser;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.ByteArrayInputStream;
import org.apache.sax.parser.XmlInputFormat;
import org.w3c.dom.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by rishikapoor on 11/11/2015.
 */
public class XmlParsingMap extends Mapper<LongWritable, Text, Text, MapWritable> {

    @Override
    protected void map(LongWritable key, Text value,
                       Context context)
            throws
            IOException, InterruptedException {
       // System.out.println("Enter Mapper");
        String document = value.toString();
      // System.out.println("‘" + document + "‘");
        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
                    .newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document doc = docBuilder.parse((new
                    ByteArrayInputStream(document.getBytes())));
            MapWritable src = new MapWritable();
            Text contextKey = new Text("Language");


            NodeList nodeList = doc.getElementsByTagName("*");
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                //System.out.println(node.getNodeName() + "  " + node.getTextContent() );


                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    // do something with the current element
                   // System.out.println(node.getNodeName() + "  " + node.getTextContent() );


                    NamedNodeMap attributes = node.getAttributes();

                    if(attributes.getLength() == 1)
                    {
                        //System.out.println(attributes.getLength());
                        Node theAttribute = attributes.item(0);
                       // contextKey.set("Language");
                        src.put(new Text(theAttribute.getNodeName().trim().getBytes("UTF-8")), new Text(theAttribute.getNodeValue().trim().getBytes("UTF-8")));

                        //tagAnValues.put(node.getNodeName(), node.getTextContent());


                    }
                    else if (attributes.getLength() >= 1) {
                        contextKey.set("LanguageName");
                       // System.out.println(">>> |" + nodeText(node) + "| >>> |" + new String(nodeText(node).getBytes(StandardCharsets.UTF_8)));

                        src.put(new Text(node.getNodeName().trim().getBytes("UTF-8")), new Text(nodeText(node).getBytes("UTF-8")));
                        //System.out.println(node.getNodeName()+ "=" + node.getTextContent());
                        for (int a = 0; a < attributes.getLength(); a++) {
                            Node theAttribute = attributes.item(a);
                            //System.out.println(theAttribute.getNodeName() + "=" + theAttribute.getNodeValue());
                            src.put(new Text(theAttribute.getNodeName().trim().getBytes("UTF-8")),new Text(theAttribute.getNodeValue().trim().getBytes("UTF-8")));
                        }
                        context.write(contextKey,src);
                    }else {
                        src.put(new Text(node.getNodeName().trim().getBytes("UTF-8")), new Text(nodeText(node).getBytes("UTF-8")));

                    }

                }

            }
            contextKey.set("Language");
            context.write(contextKey,src);

        }
        catch(Exception e){
            throw new IOException(e);

        }

    }

    private String nodeText(final Node node) {

        final StringBuilder builder = new StringBuilder();
        if (node.hasChildNodes()) {
            final NodeList childNodes = node.getChildNodes();
            for (int j = 0; j < childNodes.getLength(); j++) {
                final Node childNode = childNodes.item(j);
                if (childNode.getNodeType() == Node.TEXT_NODE) {
                    if(builder.length() > 0) {
                        builder.append('\n');
                    }
                    builder.append(childNode.getTextContent().trim());
                }
            }
        }
            return builder.toString();
    }
}
