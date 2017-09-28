package org.apache.jena.instrument;

/**
 * Created by rishikapoor on 11/01/2016.
 */

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.jena.graph.Node;
import org.apache.jena.hadoop.rdf.types.QuadWritable;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.core.Quad;

import java.io.IOException;


/**
 * Created by rishikapoor on 20/11/2015.
 */
public class InstReducer<TKey, TValue> extends Reducer<TKey, QuadWritable, TKey, Text> {

    private org.apache.hadoop.mapreduce.lib.output.MultipleOutputs<TKey, Text> multipleOutputs;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        multipleOutputs = new org.apache.hadoop.mapreduce.lib.output.MultipleOutputs<TKey,Text>(context);

        //context.write(new Text("<configuration>"), null);
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        //context.write(new Text("</configuration>"), null);
        multipleOutputs.close();
    }

    @Override
    protected void reduce(TKey key, Iterable<QuadWritable> values, Context context)
            throws IOException, InterruptedException {

        //System.out.println(key.toString());
        Model model = ModelFactory.createDefaultModel();
        for (QuadWritable qw: values) {
            Quad quad = qw.get();
            //System.out.println(quad.toString());
            Node subject = quad.getSubject();
            RDFNode rdfNode = model.asRDFNode(subject);
            Resource resource = rdfNode.asResource();
            Node object = quad.getObject();
            if (object.isLiteral()) {
                model.add(model.createStatement(model.createResource(quad.getSubject().toString()), model.createProperty(quad.getPredicate().toString()), model.createLiteral(quad.getObject().toString())));
            } else {
                model.add(model.createStatement(model.createResource(quad.getSubject().toString()), model.createProperty(quad.getPredicate().toString()), model.createResource(quad.getObject().toString())));
            }
        }

        String queryString = "DESCRIBE *\n" +
                "{\n" +
                "    { ?s ?p ?o }\n" +
                "}";



        Query query = QueryFactory.create(queryString);
        try{
            QueryExecution qexec = QueryExecutionFactory.create(query, model);
            ResultSet results = qexec.execSelect();
             //ResultSetFormatter.out(results);
            for ( ; results.hasNext() ; )
            {
                QuerySolution soln = results.nextSolution() ;
                System.out.println(soln.toString());
        }
    }catch(Exception ex){};


        String queryString1 = "prefix ont: <http://ont.thomsonreuters.com/>\n" +
                "prefix md: <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/>\n" +
                "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "prefix go: <http://ld.test.data.thomsonreuters.com/>\n" +
                "prefix co: <http://data.schemas.financial.thomsonreuters.com/Common/2009-09-01/>\n" +
                "SELECT ?instrumentId ?cFICode \n" +
                "\n" +
                "WHERE {\n" +
                "\t?x rdf:type <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/Instrument> .\n" +
                "\t?x ont:permId ?instrumentId .\n" +
                "\t?x md:cFICode ?cFICode ." +
                " } \n" ;

        try
        {
            QueryExecution qExec = QueryExecutionFactory.create(queryString1,model);
            ResultSet results = qExec.execSelect() ;
            // ResultSetFormatter.out(results) ;
            for ( ; results.hasNext() ; )
            {
                QuerySolution soln = results.nextSolution() ;
                StringBuilder sb = new StringBuilder();
                sb.append(format(soln.get("instrumentId").asNode())).append(';');

                RDFNode cFICode = soln.get("cFICode");
                if(cFICode == null)
                {
                    sb.append(("None")).append(';');
                }else
                {
                    sb.append(format(soln.get("cFICode").asNode())).append(';');
                }

                String output = sb.toString();
                output = output.replace('"',' ');
                multipleOutputs.write("Instrument", null, new Text(output.trim()));

            }
        }catch(Exception ex){};



        String queryString2 = "prefix ont: <http://ont.thomsonreuters.com/>\n" +
                "prefix md: <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/>\n" +
                "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "prefix go: <http://ld.test.data.thomsonreuters.com/>\n" +
                "prefix co: <http://data.schemas.financial.thomsonreuters.com/Common/2009-09-01/>\n" +
                "SELECT ?instrumentId ?instrumentAssetClass ?value ?effectiveFrom ?effectiveTo ?effectiveFromNACode ?effectiveToNACode ?objectTypeId\n" +
                "\n" +
                "WHERE {\n" +
                "\t?x rdf:type <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/Instrument> .\n" +
                "\t?x ont:permId ?instrumentId .\n" +
                "\t?x md:instrumentAssetClassId ?instrumentAssetClass .\n" +
                "\t?instrumentAssetClass md:instrumentAssetClassId ?value .\n" +
                "\tOPTIONAL{?instrumentAssetClass co:effectiveFrom ?effectiveFrom} .\n" +
                "\tOPTIONAL{?instrumentAssetClass co:effectiveTo ?effectiveTo} .\n" +
                "\tOPTIONAL{?instrumentAssetClass co:effectiveFromNACode ?effectiveFromNACode} .\n" +
                "\tOPTIONAL{?instrumentAssetClass co:effectiveToNACode ?effectiveToNACode} .\n" +
                "\tOPTIONAL{?instrumentAssetClass md:objectTypeId ?objectTypeId} .\n" +
                //"\t?geotype md:geographyType ?geoVal .\n" +
                "}\n";
        Query query2 = QueryFactory.create(queryString2);
        try{
            QueryExecution qexec = QueryExecutionFactory.create(query2, model);
            ResultSet results = qexec.execSelect();
            //ResultSetFormatter.out(results) ;
            for ( ; results.hasNext() ; )
            {
                //                System.out.println(soln.toString());


                QuerySolution soln = results.nextSolution() ;
                StringBuilder sb = new StringBuilder();
                sb.append(format(soln.get("instrumentId").asNode())).append(';');

                RDFNode value = soln.get("value");
                if(value == null)
                {
                    sb.append(("None")).append(';');
                }else
                {
                    sb.append(format(soln.get("value").asNode())).append(';');
                }


                RDFNode instrumentAssetClass = soln.get("instrumentAssetClass");
                if(instrumentAssetClass == null)
                {
                    sb.append(("None")).append(';');
                }else
                {
                    sb.append(format(soln.get("instrumentAssetClass").asNode())).append(';');
                }

                RDFNode effectiveFrom =  soln.get("effectiveFrom");
                if(effectiveFrom == null)
                {
                    sb.append(("None")).append(';');
                }else
                {
                    sb.append(format(soln.get("effectiveFrom").asNode())).append(';');
                }

                RDFNode effectiveTo = soln.get("effectiveTo");
                if(effectiveTo == null)
                {
                    sb.append("None").append(';');
                }else
                {
                    sb.append(format(soln.get("effectiveTo").asNode())).append(';');
                }

                RDFNode effectiveFromNACode = soln.get("effectiveFromNACode");
                if(effectiveFromNACode == null)
                {
                    sb.append(("None")).append(';');
                }else{

                    sb.append(format(soln.get("effectiveFromNACode").asNode())).append(';');
                }

                RDFNode effectiveToNACode = soln.get("effectiveToNACode");
                if(effectiveToNACode == null)
                {
                    sb.append(("None")).append(';');
                }else{

                    sb.append(format(soln.get("effectiveToNACode").asNode())).append(';');
                }
                RDFNode objectTypeId = soln.get("objectTypeId");
                if(objectTypeId == null)
                {
                    sb.append(("None")).append(';');
                }else{

                    sb.append(format(soln.get("objectTypeId").asNode())).append(';');
                };


                String output = sb.toString();
                output = output.replace('"',' ');
                multipleOutputs.write("InstrumentAssetClassId", null, new Text(output.trim()));
                //context.write(key, new Text(output));
                //System.out.println(output);
            }
        }catch(Exception ex){};



        String queryString3 = "prefix ont: <http://ont.thomsonreuters.com/>\n" +
                "prefix md: <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/>\n" +
                "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "prefix go: <http://ld.test.data.thomsonreuters.com/>\n" +
                "prefix xml: <xml>\n"+
                "SELECT ?instrumentId ?instrumentCommonName ?value ?effectiveFrom ?effectiveTo ?effectiveFromNACode ?effectiveToNACode ?language\n" +
                "\n" +
                "WHERE {\n" +
                "\t?x rdf:type <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/Instrument> .\n" +
                "\t?x ont:permId ?instrumentId .\n" +
                "\t?x md:instrumentCommonName ?instrumentCommonName .\n" +
                "\t?instrumentCommonName rdf:value ?value.\n" +
                "\tOPTIONAL{?instrumentCommonName md:effectiveFrom ?effectiveFrom} .\n" +
                "\tOPTIONAL{?instrumentCommonName md:effectiveTo ?effectiveTo} .\n" +
                "\tOPTIONAL{?instrumentCommonName md:effectiveFromNACode ?effectiveFromNACode} .\n" +
                "\tOPTIONAL{?instrumentCommonName md:effectiveToNACode ?effectiveToNACode} .\n" +
                "\tOPTIONAL{?instrumentCommonName <xml:lang> ?language} .\n" +
                //"\t?geotype md:geographyType ?geoVal .\n" +
                "}\n";

        Query query3 = QueryFactory.create(queryString3);
        try{
            QueryExecution qexec = QueryExecutionFactory.create(query3, model);
            ResultSet results = qexec.execSelect();

            for ( ; results.hasNext() ; )
            {
                //                System.out.println(soln.toString());


                QuerySolution soln = results.nextSolution() ;
                StringBuilder sb = new StringBuilder();
                sb.append(format(soln.get("instrumentId").asNode())).append(';');

                RDFNode value = soln.get("value");
                if(value == null)
                {
                    sb.append(("None")).append(';');
                }else
                {
                    sb.append(format(soln.get("value").asNode())).append(';');
                }

                RDFNode effectiveFrom =  soln.get("effectiveFrom");
                if(effectiveFrom == null)
                {
                    sb.append(("None")).append(';');
                }else
                {
                    sb.append(format(soln.get("effectiveFrom").asNode())).append(';');
                }

                RDFNode effectiveTo = soln.get("effectiveTo");
                if(effectiveTo == null)
                {
                    sb.append("None").append(';');
                }else
                {
                    sb.append(format(soln.get("effectiveTo").asNode())).append(';');
                }

                RDFNode effectiveFromNACode = soln.get("effectiveFromNACode");
                if(effectiveFromNACode == null)
                {
                    sb.append(("None")).append(';');
                }else{

                    sb.append(format(soln.get("effectiveFromNACode").asNode())).append(';');
                }

                RDFNode effectiveToNACode = soln.get("effectiveToNACode");
                if(effectiveToNACode == null)
                {
                    sb.append(("None")).append(';');
                }else{

                    sb.append(format(soln.get("effectiveToNACode").asNode())).append(';');
                }
                RDFNode language = soln.get("language");
                if(language == null)
                {
                    sb.append(("None")).append(';');
                }else{

                    sb.append(format(soln.get("language").asNode())).append(';');
                }

                String output = sb.toString();
                output = output.replace('"',' ');
                multipleOutputs.write("InstrumentCommonName", null, new Text(output.trim()));
                //context.write(key, new Text(output));
                //System.out.println(output);
            }
        }catch(Exception ex){};

        String queryString4 = "prefix ont: <http://ont.thomsonreuters.com/>\n" +
                "prefix md: <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/>\n" +
                "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "prefix go: <http://ld.test.data.thomsonreuters.com/>\n" +
                "prefix xml: <xml>\n"+
                "SELECT ?instrumentId ?instrumentStatus ?value ?effectiveFrom ?effectiveTo ?effectiveFromNACode ?effectiveToNACode \n" +
                "\n" +
                "WHERE {\n" +
                "\t?x rdf:type <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/Instrument> .\n" +
                "\t?x ont:permId ?instrumentId .\n" +
                "\t?x md:instrumentStatus ?instrumentStatus .\n" +
                "\t?instrumentStatus md:instrumentStatus ?value.\n" +
                "\tOPTIONAL{?instrumentStatus md:effectiveFrom ?effectiveFrom} .\n" +
                "\tOPTIONAL{?instrumentStatus md:effectiveTo ?effectiveTo} .\n" +
                "\tOPTIONAL{?instrumentStatus md:effectiveFromNACode ?effectiveFromNACode} .\n" +
                "\tOPTIONAL{?instrumentStatus md:effectiveToNACode ?effectiveToNACode} .\n" +
                //"\t?geotype md:geographyType ?geoVal .\n" +
                "}\n";

        Query query4 = QueryFactory.create(queryString4);
        try{
            QueryExecution qexec = QueryExecutionFactory.create(query4, model);
            ResultSet results = qexec.execSelect();
            //ResultSetFormatter.out(results);
            for ( ; results.hasNext() ; )
            {
                //                System.out.println(soln.toString());


                QuerySolution soln = results.nextSolution() ;
                StringBuilder sb = new StringBuilder();
                sb.append(format(soln.get("instrumentId").asNode())).append(';');

                RDFNode value = soln.get("value");
                if(value == null)
                {
                    sb.append(("None")).append(';');
                }else
                {
                    sb.append(format(soln.get("value").asNode())).append(';');
                }

                RDFNode effectiveFrom =  soln.get("effectiveFrom");
                if(effectiveFrom == null)
                {
                    sb.append(("None")).append(';');
                }else
                {
                    sb.append(format(soln.get("effectiveFrom").asNode())).append(';');
                }

                RDFNode effectiveTo = soln.get("effectiveTo");
                if(effectiveTo == null)
                {
                    sb.append("None").append(';');
                }else
                {
                    sb.append(format(soln.get("effectiveTo").asNode())).append(';');
                }

                RDFNode effectiveFromNACode = soln.get("effectiveFromNACode");
                if(effectiveFromNACode == null)
                {
                    sb.append(("None")).append(';');
                }else{

                    sb.append(format(soln.get("effectiveFromNACode").asNode())).append(';');
                }

                RDFNode effectiveToNACode = soln.get("effectiveToNACode");
                if(effectiveToNACode == null)
                {
                    sb.append(("None")).append(';');
                }else{

                    sb.append(format(soln.get("effectiveToNACode").asNode())).append(';');
                };

                String output = sb.toString();
                output = output.replace('"',' ');
                multipleOutputs.write("InstrumentStatus", null, new Text(output.trim()));
                //context.write(key, new Text(output));
                //System.out.println(output);
            }
        }catch(Exception ex){};

        String queryString5 = "prefix ont: <http://ont.thomsonreuters.com/>\n" +
                "prefix md: <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/>\n" +
                "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "prefix go: <http://ld.test.data.thomsonreuters.com/>\n" +
                "prefix xml: <xml>\n"+
                "SELECT ?instrumentId ?davRcsAssetClass ?value ?effectiveFrom ?effectiveTo ?effectiveFromNACode ?effectiveToNACode \n" +
                "\n" +
                "WHERE {\n" +
                "\t?x rdf:type <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/Instrument> .\n" +
                "\t?x ont:permId ?instrumentId .\n" +
                "\t?x md:davRcsAssetClass ?davRcsAssetClass .\n" +
                "\t?davRcsAssetClass rdf:value ?value.\n" +
                "\tOPTIONAL{?davRcsAssetClass md:effectiveFrom ?effectiveFrom} .\n" +
                "\tOPTIONAL{?davRcsAssetClass md:effectiveTo ?effectiveTo} .\n" +
                "\tOPTIONAL{?davRcsAssetClass md:effectiveFromNACode ?effectiveFromNACode} .\n" +
                "\tOPTIONAL{?davRcsAssetClass md:effectiveToNACode ?effectiveToNACode} .\n" +
                //"\t?geotype md:geographyType ?geoVal .\n" +
                "}\n";


        Query query5 = QueryFactory.create(queryString5);
        try{
            QueryExecution qexec = QueryExecutionFactory.create(query5, model);
            ResultSet results = qexec.execSelect();
            //ResultSetFormatter.out(results);
            for ( ; results.hasNext() ; )
            {
                //                System.out.println(soln.toString());


                QuerySolution soln = results.nextSolution() ;
                StringBuilder sb = new StringBuilder();
                sb.append(format(soln.get("instrumentId").asNode())).append(';');

                RDFNode value = soln.get("value");
                if(value == null)
                {
                    sb.append(("None")).append(';');
                }else
                {
                    sb.append(format(soln.get("value").asNode())).append(';');
                }

                RDFNode effectiveFrom =  soln.get("effectiveFrom");
                if(effectiveFrom == null)
                {
                    sb.append(("None")).append(';');
                }else
                {
                    sb.append(format(soln.get("effectiveFrom").asNode())).append(';');
                }

                RDFNode effectiveTo = soln.get("effectiveTo");
                if(effectiveTo == null)
                {
                    sb.append("None").append(';');
                }else
                {
                    sb.append(format(soln.get("effectiveTo").asNode())).append(';');
                }

                RDFNode effectiveFromNACode = soln.get("effectiveFromNACode");
                if(effectiveFromNACode == null)
                {
                    sb.append(("None")).append(';');
                }else{

                    sb.append(format(soln.get("effectiveFromNACode").asNode())).append(';');
                }

                RDFNode effectiveToNACode = soln.get("effectiveToNACode");
                if(effectiveToNACode == null)
                {
                    sb.append(("None")).append(';');
                }else{

                    sb.append(format(soln.get("effectiveToNACode").asNode())).append(';');
                };

                String output = sb.toString();
                output = output.replace('"',' ');
                multipleOutputs.write("DavRcsAssetClass", null, new Text(output.trim()));
                //context.write(key, new Text(output));
                //System.out.println(output);
            }
        }catch(Exception ex){};


    }

    public String format(Node n)
    {
        String URIVal = null;
        if ( n.isBlank() )
            URIVal = formatBNode(n) ;
        else if ( n.isURI() ) {
            URIVal = formatURI(n);
        }
        else if ( n.isLiteral() )
            URIVal = formatLiteral(n) ;
        else if ( n.isVariable() )
            formatVar(n) ;
        else if ( Node.ANY.equals(n) )
            URIVal = "ANY" ;
        else
            throw new ARQInternalErrorException("Unknow node type: "+n) ;

        return URIVal;
    }

    public String formatURI(Node n) {

        String getUrl = n.getURI();
        String URIVal = null;
        //System.out.println("The url is " + getUrl);

        if (getUrl.contains("#")) {
            URIVal = n.getLocalName();
        }else if(getUrl.contains("xml"))
        {
            String value[] = getUrl.split(":");
            URIVal = value[value.length-1];
        }else if(getUrl.contains("^^"))
        {
            String value[] = getUrl.split("^^");
            URIVal = value[0];
        } else
        {
            String value[] = getUrl.split("/");
            URIVal = value[value.length-1];
            if(URIVal.contains("-"))
            {
                String spliFurther[] = URIVal.split("-");
                URIVal = spliFurther[spliFurther.length-1];

            }
        }

        return URIVal;
    }


    public String formatBNode(Node n)       {
        String lab = NodeFmtLib.encodeBNodeLabel(n.getBlankNodeLabel());
        return lab;
    }


    public String formatLiteral(Node n)
    {
        RDFDatatype dt = n.getLiteralDatatype() ;
        String lang = n.getLiteralLanguage() ;
        String lex = n.getLiteralLexicalForm() ;
        String value;

        String tempvalue[] = lex.split("\\^\\^");
        value = tempvalue[0] ;
        return value;
    }


    public String formatVar(Node n)         {
        return n.getName() ; }

}



