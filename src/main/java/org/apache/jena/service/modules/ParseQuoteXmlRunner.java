package org.apache.jena.service.modules;

import org.apache.sax.quoteParser.ParseQuoteXml;

/**
 * Created by rishikapoor on 14/01/2016.
 */
public class ParseQuoteXmlRunner
{
    public static void main (String[] args) {
        new ParseQuoteXml().setArgs(args).run();
    }
}
