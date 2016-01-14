package org.apache.jena.service.modules;

import org.apache.sax.instParser.ParseInstXml;


/**
 * Created by rishikapoor on 14/01/2016.
 */
public class ParseInstXmlRunner {
    public static void main (String[] args) {
        new ParseInstXml().setArgs(args).run();
    }
}
