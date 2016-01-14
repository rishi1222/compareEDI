package org.apache.jena.service.modules;

import org.apache.jena.instrument.ParseInstRdf;


/**
 * Created by rishikapoor on 14/01/2016.
 */
public class ParseInstRdfRunner {
    public static void main (String[] args) {
        new ParseInstRdf().setArgs(args).run();
    }
}
