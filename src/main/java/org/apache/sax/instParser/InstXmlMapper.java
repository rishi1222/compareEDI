package org.apache.sax.instParser;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Created by rishikapoor on 11/01/2016.
 */
public class InstXmlMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

    @Override
    protected void map(LongWritable key, Text value,
                       Context context)
            throws
            IOException, InterruptedException {
        // System.out.println("Enter Mapper");
        String document = value.toString();
        //System.out.println("‘" + document + "‘");
        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
                    .newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document doc = docBuilder.parse((new
                    ByteArrayInputStream(document.getBytes())));
            MapWritable src = new MapWritable();
            MapWritable newsrc = new MapWritable();
            String instrumentId = null;
            String nodeName = null;
            Text contextKey = new Text("Language");


            NodeList nodeList = doc.getElementsByTagName("*");
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                //System.out.println(node.getNodeName() + "  " + node.getTextContent() );


                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    // do something with the current element
                    //System.out.println(node.getNodeName() + "  " + node.getTextContent() );


                    NamedNodeMap attributes = node.getAttributes();

                    if(attributes.getLength() == 1)
                    {
                        //System.out.println(attributes.getLength());
                        Node theAttribute = attributes.item(0);
                        newsrc.put(new Text(theAttribute.getNodeName().trim().getBytes("UTF-8")), new Text(theAttribute.getNodeValue().trim().getBytes("UTF-8")));
                        newsrc.put(new Text(node.getNodeName().trim().getBytes("UTF-8")), new Text(node.getTextContent().getBytes("UTF-8")));
                        if(node.getNodeName().equalsIgnoreCase("InstrumentId"))
                        {
                            instrumentId = node.getTextContent();
                            nodeName = node.getNodeName();
                        }

                        //tagAnValues.put(node.getNodeName(), node.getTextContent());


                    }
                    else if (attributes.getLength() >= 1) {
                        contextKey.set(node.getNodeName());
                        // System.out.println(">>> |" + nodeText(node) + "| >>> |" + new String(nodeText(node).getBytes(StandardCharsets.UTF_8)));

                        src.put(new Text(node.getNodeName().trim().getBytes("UTF-8")), new Text(node.getTextContent().getBytes("UTF-8")));
                        //System.out.println(node.getNodeName()+ "=" + node.getTextContent());
                        for (int a = 0; a < attributes.getLength(); a++) {
                            Node theAttribute = attributes.item(a);
                            //System.out.println(theAttribute.getNodeName() + "=" + theAttribute.getNodeValue());
                            src.put(new Text(theAttribute.getNodeName().trim().getBytes("UTF-8")),new Text(theAttribute.getNodeValue().trim().getBytes("UTF-8")));
                        }
                        src.put(new Text(nodeName.getBytes("UTF-8")), new Text(instrumentId.getBytes("UTF-8")));
                        context.write(contextKey,src);
                        src.clear();
                    }else {
                        newsrc.put(new Text(node.getNodeName().trim().getBytes("UTF-8")), new Text(node.getTextContent().getBytes("UTF-8")));
                        //System.out.println(node.getNodeName() + "  " + node.getTextContent());

                    }

                }

            }
            contextKey.set("Language");
            context.write(contextKey,newsrc);

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
