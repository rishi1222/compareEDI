package org.apache.jena.service.modules;

import org.apache.jena.quote.ParseQuoteRdf;

/**
 * Created by rishikapoor on 14/01/2016.
 */
public class ParseQuoteRdfRunner {
    public static void main (String[] args) {
        new ParseQuoteRdf().setArgs(args).run();
    }
}
