package org.apache.jena.service;

// Imports
///////////////
import org.apache.hadoop.util.ToolRunner;
import org.apache.hdfs.files.ReadNquadFiles;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.out.SinkQuadBracedOutput;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFCountingBase;
import org.apache.jena.riot.system.StreamRDFWrapper;
import org.apache.jena.riot.writer.WriterStreamRDFFlat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * <p>TODO class comment</p>
 */

/**
 * Created by rishikapoor on 29/09/2015.
 */
public class parseGeoRDF extends Base {

    /***********************************/
    /* Constants                       */
    /***********************************/

    // Directory where we've stored the local data files, such as pizza.rdf.owl
    public static final String SOURCE = "./src/main/resources/rdf_data/";
    public static final String TDB_SOURCE = "./src/main/resources/TDB_DATA";

    // Pizza ontology namespace
    public static final String PIZZA_NS = "http://www.co-ode.org/ontologies/pizza/pizza.owl#";
    public static final String ORGANIZATION_NS = "http://data.thomsonreuters.com";

    /***********************************/
    /* Static variables                */
    /***********************************/

    @SuppressWarnings(value = "unused")
    private static final Logger log = LoggerFactory.getLogger(parseGeoRDF.class);
    private static final Lang TURTLE = RDFLanguages.TURTLE;

    int returnCode;

    /***********************************/
    /* Instance variables              */
    /***********************************/

    /***********************************/
    /* Constructors                    */
    /***********************************/

    /***********************************/
    /* External signature methods      */
    /***********************************/

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception{

        int exitCode;

        exitCode = ToolRunner.run(new parseGeoRDF(), args);

        System.exit(exitCode);

    }

    public int run(String[] args) throws Exception {

        System.err.printf("Usage: %s [generic options] <input> <output>\n",
                getClass().getSimpleName());
        ToolRunner.printGenericCommandUsage(System.err);
       // returnCode = ToolRunner.run(new org.apache.sax.instParser.ParseInstXml(), args);
        //returnCode = ToolRunner.run(new org.apache.sax.quoteParser.ParseQuoteXml(), args);
        returnCode = ToolRunner.run(new org.apache.jena.instrument.ParseInstRdf(),args);
        //new LookUpParser().setArgs(args).run();
        //new parseGeoRDF().setArgs(args).run();
        //new org.apache.sax.parser.ParseQuoteXml().setArgs(args).run();
        //new org.apache.sax.quoteParse.ParseQuoteXml().setArgs(args).run();
        // new org.apache.sax.instParser.ParseInstXml().setArgs(args).run();
        //new ParseRdfMpRd().setArgs(args).run();
        //new ParseQuoteRdf().setArgs(args).run();
        //new ParseInstRdf().setArgs(args).run();
        //new HiveJdbcClient().setArgs(args).run();
        //new compareTextFiles().setArgs(args).run();
        return returnCode;
    }

    public void run() {


        ReadNquadFiles readFile;


        try {

            readFile = new ReadNquadFiles();
            //InputStream fileInput = readFile.getHDFSinputFiles();
            InputStream fileInput = new FileInputStream("./src/xxx");
            OutputStream outputFile = null;
            try {
                outputFile = new FileOutputStream("./src/xxx");
            } catch (Exception e) {
                e.printStackTrace();
            }


            StreamRDF destination = new WriterStreamRDFFlat(outputFile);
            SinkQuadBracedOutput quadBracedOutput = new SinkQuadBracedOutput(System.out);
            StreamRDFWrapper operation = new StreamRDFCountingBase(new WriterStreamRDFFlat(System.out));
            RDFDataMgr.parse(destination, fileInput, "", RDFLanguages.NQUADS);

            fileInput.close();
            outputFile.flush();
            outputFile.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


 /*
        Iterator<String> name  = dataset.listNames();
        while(name.hasNext())
        {
            System.out.println(name.next());
        }


        String qs1 = "select (count(*) as ?count) {\n" +
                "    graph ?g {\n" +
                "       ?s <http://data.schemas.financial.thomsonreuters.com/metadata/2009-09-01/geographyId> ?o\n" +
                "    }\n" +
                "}";

        try{
            QueryExecution qExec = QueryExecutionFactory.create(qs1, dataset);
            ResultSet rs = qExec.execSelect() ;
            ResultSetFormatter.out(rs) ;
        }catch(Exception ex){};


        String qs2 = "Select ?g ?o (count(*) as ?count) {\n" +
                "  graph ?g {\n" +
                "    ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o\n" +
                "  }\n" +
                "}\n" +
                "group by ?g ?o\n" +
                "order by ?g ";

        try
        {
            QueryExecution qExec = QueryExecutionFactory.create(qs2, dataset);
            ResultSet rs = qExec.execSelect() ;
            ResultSetFormatter.out(rs) ;
        }catch(Exception ex){};

     */
    }
}
