package org.apache.jena.quote;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.hadoop.rdf.types.QuadWritable;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.core.Quad;

import java.io.IOException;

/**
 * Created by rishikapoor on 11/01/2016.
 */
public class QuoteReducer<TKey, TValue> extends Reducer<TKey, QuadWritable, TKey, Text> {

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
                model.add(model.createStatement(model.createResource(quad.getSubject().toString()), model.createProperty(quad.getPredicate().toString()), model.createResource(quad.getObject().toString(), model.createResource(quad.getGraph().toString()))));
            }
        }

        String queryString = "SELECT *\n" +
                "{\n" +
                "    { ?s ?p ?o }\n" +
                "}";



        Query query = QueryFactory.create(queryString);
        try{
            QueryExecution qexec = QueryExecutionFactory.create(query, model);
            ResultSet results = qexec.execSelect();
            // ResultSetFormatter.out(results);
            for ( ; results.hasNext() ; )
            {
                QuerySolution soln = results.nextSolution() ;


//                System.out.println(soln.toString());
//
//
//
//                StringBuilder sb = new StringBuilder();
//                sb.append(format(soln.get("langId").asNode())).append(';').append(format(soln.get("activeFrom").asNode())).append(';');
//
//                RDFNode baseLanguageId =  soln.get("baseLanguageId");
//                if(baseLanguageId == null)
//                {
//                    sb.append(("None")).append(';');
//                }else
//                {
//                    sb.append(format(soln.get("baseLanguageId").asNode())).append(';');
//                }
//
//                RDFNode languageGeographyId = soln.get("languageGeographyId");
//                if(languageGeographyId == null)
//                {
//                    sb.append("None").append(';');
//                }else
//                {
//                    sb.append(format(soln.get("languageGeographyId").asNode())).append(';');
//                }
//                sb.append(format(soln.get("languageUniqueName").asNode())).append(';');
//                RDFNode languageScriptId = soln.get("languageScriptId");
//                if(languageScriptId == null)
//                {
//                    sb.append(("None")).append(';');
//                }else{
//
//                    sb.append(format(soln.get("languageScriptId").asNode())).append(';');
//                }
//
//                ;
//
//                String output = sb.toString();
//                output = output.replace('"',' ');
//                multipleOutputs.write("Lang", null, new Text(output.trim()));
//                //context.write(key, new Text(output));
//                System.out.println(output);
            }
        }catch(Exception ex){};

        String qs2 = "prefix ont: <http://ont.thomsonreuters.com/>\n" +
                "                prefix md: <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/> \n" +
                "                prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
                "                prefix go: <http://ld.test.data.thomsonreuters.com/>\n " +
                "                prefix mf: <http://data.schemas.financial.thomsonreuters.com/Common/2009-09-01/> \n"+
                "SELECT ?quoteId ?callPutOption \n" +
                " where \n" +
                "    { ?s rdf:type <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/Quote>. " +
                "?s  ont:permId ?quoteId. \n" +
                " OPTIONAL{?s md:callPutOption ?callPutOption}." +
                " } \n" ;

        try
        {
            QueryExecution qExec = QueryExecutionFactory.create(qs2,model);
            ResultSet results = qExec.execSelect() ;
            //ResultSetFormatter.out(results) ;
            for ( ; results.hasNext() ; )
            {
                QuerySolution soln = results.nextSolution() ;
                //System.out.println(soln.toString());
            }
        }catch(Exception ex){};

        String queryString1 = "prefix ont: <http://ont.thomsonreuters.com/>\n" +
                "prefix md: <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/>\n" +
                "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "prefix go:  <http://data.schemas.financial.thomsonreuters.com/Common/2009-09-01/>\n" +
                "SELECT ?quoteId ?mIC ?value ?effectiveFrom ?effectiveTo ?effectiveFromNACode ?effectiveToNACode\n" +
                "\n" +
                "WHERE {\n" +
                "\t?x rdf:type <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/Quote> .\n" +
                "\t?x ont:permId ?quoteId .\n" +
                "\t?x md:mIC ?mIC .\n" +
                "\t?mIC rdf:value ?value .\n" +
                "\tOPTIONAL{?mIC md:effectiveFrom ?effectiveFrom} .\n" +
                "\tOPTIONAL{?mIC md:effectiveTo ?effectiveTo} .\n" +
                "\tOPTIONAL{?mIC md:effectiveFromNACode ?effectiveFromNACode} .\n" +
                "\tOPTIONAL{?mIC md:effectiveToNACode ?effectiveToNACode} .\n" +
                //"\t?geotype md:geographyType ?geoVal .\n" +
                "}\n";

        Query query1 = QueryFactory.create(queryString1);
        try{
            QueryExecution qexec = QueryExecutionFactory.create(query1, model);
            ResultSet results = qexec.execSelect();
            //ResultSetFormatter.out(results);
            for ( ; results.hasNext() ; )
            {
                //                System.out.println(soln.toString());


                QuerySolution soln = results.nextSolution() ;
                StringBuilder sb = new StringBuilder();
                sb.append(format(soln.get("quoteId").asNode())).append(';');

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
                multipleOutputs.write("MIC", null, new Text(output.trim()));
                //context.write(key, new Text(output));
                //System.out.println(output);
            }
        }catch(Exception ex){};


        String queryString2 = "prefix ont: <http://ont.thomsonreuters.com/>\n" +
                "prefix md: <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/>\n" +
                "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "prefix go: <http://ld.test.data.thomsonreuters.com/>\n" +
                "prefix co: <http://data.schemas.financial.thomsonreuters.com/Common/2009-09-01/>\n" +
                "SELECT ?quoteId ?quoteAssetClass ?value ?effectiveFrom ?effectiveTo ?effectiveFromNACode ?effectiveToNACode ?objectTypeId\n" +
                "\n" +
                "WHERE {\n" +
                "\t?x rdf:type <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/Quote> .\n" +
                "\t?x ont:permId ?quoteId .\n" +
                "\t?x md:quoteAssetClassId ?quoteAssetClass .\n" +
                "\t?quoteAssetClass md:quoteAssetClassId ?value .\n" +
                "\tOPTIONAL{?quoteAssetClass co:effectiveFrom ?effectiveFrom} .\n" +
                "\tOPTIONAL{?quoteAssetClass co:effectiveTo ?effectiveTo} .\n" +
                "\tOPTIONAL{?quoteAssetClass co:effectiveFromNACode ?effectiveFromNACode} .\n" +
                "\tOPTIONAL{?quoteAssetClass co:effectiveToNACode ?effectiveToNACode} .\n" +
                "\tOPTIONAL{?quoteAssetClass md:objectTypeId ?objectTypeId} .\n" +
                //"\t?geotype md:geographyType ?geoVal .\n" +
                "}\n";

        Query query2 = QueryFactory.create(queryString2);
        try{
            QueryExecution qexec = QueryExecutionFactory.create(query2, model);
            ResultSet results = qexec.execSelect();
            for ( ; results.hasNext() ; )
            {
                //                System.out.println(soln.toString());


                QuerySolution soln = results.nextSolution() ;
                StringBuilder sb = new StringBuilder();
                sb.append(format(soln.get("quoteId").asNode())).append(';');

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
                RDFNode objectTypeId = soln.get("objectTypeId");
                if(effectiveToNACode == null)
                {
                    sb.append(("None")).append(';');
                }else{

                    sb.append(format(soln.get("objectTypeId").asNode())).append(';');
                };


                String output = sb.toString();
                output = output.replace('"',' ');
                multipleOutputs.write("QuoteAssetClassId", null, new Text(output.trim()));
                //context.write(key, new Text(output));
                //System.out.println(output);
            }
        }catch(Exception ex){};



        String queryString3 = "prefix ont: <http://ont.thomsonreuters.com/>\n" +
                "prefix md: <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/>\n" +
                "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "prefix go: <http://ld.test.data.thomsonreuters.com/>\n" +
                "prefix xml: <xml>\n"+
                "SELECT ?quoteId ?quoteCommonName ?value ?effectiveFrom ?effectiveTo ?effectiveFromNACode ?effectiveToNACode ?language\n" +
                "\n" +
                "WHERE {\n" +
                "\t?x rdf:type <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/Quote> .\n" +
                "\t?x ont:permId ?quoteId .\n" +
                "\t?x md:quoteCommonName ?quoteCommonName .\n" +
                "\t?quoteCommonName rdf:value ?value.\n" +
                "\tOPTIONAL{?quoteCommonName md:effectiveFrom ?effectiveFrom} .\n" +
                "\tOPTIONAL{?quoteCommonName md:effectiveTo ?effectiveTo} .\n" +
                "\tOPTIONAL{?quoteCommonName md:effectiveFromNACode ?effectiveFromNACode} .\n" +
                "\tOPTIONAL{?quoteCommonName md:effectiveToNACode ?effectiveToNACode} .\n" +
                "\tOPTIONAL{?quoteCommonName <xml:lang> ?language} .\n" +
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
                sb.append(format(soln.get("quoteId").asNode())).append(';');

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
                multipleOutputs.write("QuoteCommonName", null, new Text(output.trim()));
                //context.write(key, new Text(output));
                //System.out.println(output);
            }
        }catch(Exception ex){};

        String queryString4 = "prefix ont: <http://ont.thomsonreuters.com/>\n" +
                "prefix md: <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/>\n" +
                "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "prefix go: <http://ld.test.data.thomsonreuters.com/>\n" +
                "SELECT ?quoteId ?quoteExchangeCode ?value ?effectiveFrom ?effectiveTo ?effectiveFromNACode ?effectiveToNACode\n" +
                "\n" +
                "WHERE {\n" +
                "\t?x rdf:type <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/Quote> .\n" +
                "\t?x ont:permId ?quoteId .\n" +
                "\t?x md:quoteExchangeCode ?quoteExchangeCode .\n" +
                "\t?quoteExchangeCode rdf:value ?value .\n" +
                "\tOPTIONAL{?quoteExchangeCode md:effectiveFrom ?effectiveFrom} .\n" +
                "\tOPTIONAL{?quoteExchangeCode md:effectiveTo ?effectiveTo} .\n" +
                "\tOPTIONAL{?quoteExchangeCode md:effectiveFromNACode ?effectiveFromNACode} .\n" +
                "\tOPTIONAL{?quoteExchangeCode md:effectiveToNACode ?effectiveToNACode} .\n" +
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
                sb.append(format(soln.get("quoteId").asNode())).append(';');

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
                multipleOutputs.write("QuoteExchangeCode", null, new Text(output.trim()));
                //context.write(key, new Text(output));
                //System.out.println(output);
            }
        }catch(Exception ex){};

        String queryString5 = "prefix ont: <http://ont.thomsonreuters.com/>\n" +
                "prefix md: <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/>\n" +
                "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "prefix go: <http://ld.test.data.thomsonreuters.com/>\n" +
                "SELECT ?quoteId ?quoteType ?value ?effectiveFrom ?effectiveTo ?effectiveFromNACode ?effectiveToNACode \n" +
                "\n" +
                "WHERE {\n" +
                "\t?x rdf:type <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/Quote> .\n" +
                "\t?x ont:permId ?quoteId .\n" +
                "\t?x md:quoteType ?quoteType .\n" +
                "\t?quoteType rdf:value ?value .\n" +
                "\tOPTIONAL{?quoteType md:effectiveFrom ?effectiveFrom} .\n" +
                "\tOPTIONAL{?quoteType md:effectiveTo ?effectiveTo} .\n" +
                "\tOPTIONAL{?quoteType md:effectiveFromNACode ?effectiveFromNACode} .\n" +
                "\tOPTIONAL{?quoteType md:effectiveToNACode ?effectiveToNACode} .\n" +
                "}\n";

        Query query5 = QueryFactory.create(queryString5);
        try{
            QueryExecution qexec = QueryExecutionFactory.create(query5, model);
            ResultSet results = qexec.execSelect();

            for ( ; results.hasNext() ; )
            {
                ResultSetFormatter.out(results);
                //                System.out.println(soln.toString());


                QuerySolution soln = results.nextSolution() ;
                StringBuilder sb = new StringBuilder();
                sb.append(format(soln.get("quoteId").asNode())).append(';');

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
                multipleOutputs.write("QuoteType", null, new Text(output.trim()));
                //context.write(key, new Text(output));
                //System.out.println(output);
            }
        }catch(Exception ex){};

        String queryString6 = "prefix ont: <http://ont.thomsonreuters.com/>\n" +
                "prefix md: <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/>\n" +
                "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "prefix go: <http://ld.test.data.thomsonreuters.com/>\n" +
                "SELECT ?quoteId ?strikePrice ?value ?effectiveFrom ?effectiveTo ?effectiveFromNACode ?effectiveToNACode\n" +
                "\n" +
                "WHERE {\n" +
                "\t?x rdf:type <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/Quote> .\n" +
                "\t?x ont:permId ?quoteId .\n" +
                "\t?x md:strikePrice ?strikePrice  .\n" +
                "\t?strikePrice rdf:value ?value .\n" +
                "\tOPTIONAL{?strikePrice md:effectiveFrom ?effectiveFrom} .\n" +
                "\tOPTIONAL{?strikePrice md:effectiveTo ?effectiveTo} .\n" +
                "\tOPTIONAL{?strikePrice md:effectiveFromNACode ?effectiveFromNACode} .\n" +
                "\tOPTIONAL{?strikePrice md:effectiveToNACode ?effectiveToNACode} .\n" +
                "}\n";

        Query query6 = QueryFactory.create(queryString6);
        try{
            QueryExecution qexec = QueryExecutionFactory.create(query6, model);
            ResultSet results = qexec.execSelect();
            //ResultSetFormatter.out(results);
            for ( ; results.hasNext() ; )
            {
                //                System.out.println(soln.toString());


                QuerySolution soln = results.nextSolution() ;
                StringBuilder sb = new StringBuilder();
                sb.append(format(soln.get("quoteId").asNode())).append(';');

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
                multipleOutputs.write("StrikePrice", null, new Text(output.trim()));
                //context.write(key, new Text(output));
                //System.out.println(output);
            }
        }catch(Exception ex){};

        String queryString7 = "prefix ont: <http://ont.thomsonreuters.com/>\n" +
                "prefix md: <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/>\n" +
                "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "prefix go: <http://ld.test.data.thomsonreuters.com/>\n" +
                "SELECT ?quoteId ?strikePriceMultiplier ?value ?effectiveFrom ?effectiveTo ?effectiveFromNACode ?effectiveToNACode\n" +
                "\n" +
                "WHERE {\n" +
                "\t?x rdf:type <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/Quote> .\n" +
                "\t?x ont:permId ?quoteId  .\n" +
                "\t?x md:strikePriceMultiplier ?strikePriceMultiplier .\n" +
                "\t?strikePriceMultiplier rdf:value ?value .\n" +
                "\tOPTIONAL{?strikePriceMultiplier md:effectiveFrom ?effectiveFrom} .\n" +
                "\tOPTIONAL{?strikePriceMultiplier md:effectiveTo ?effectiveTo} .\n" +
                "\tOPTIONAL{?strikePriceMultiplier md:effectiveFromNACode ?effectiveFromNACode} .\n" +
                "\tOPTIONAL{?strikePriceMultiplier md:effectiveToNACode ?effectiveToNACode} .\n" +
                "}\n";

        Query query7 = QueryFactory.create(queryString7);
        try{
            QueryExecution qexec = QueryExecutionFactory.create(query7, model);
            ResultSet results = qexec.execSelect();
            //ResultSetFormatter.out(results);
            for ( ; results.hasNext() ; )
            {
                //                System.out.println(soln.toString());


                QuerySolution soln = results.nextSolution() ;
                StringBuilder sb = new StringBuilder();
                sb.append(format(soln.get("quoteId").asNode())).append(';');

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
                multipleOutputs.write("StrikePriceMultiplier", null, new Text(output.trim()));
                //context.write(key, new Text(output));
                //System.out.println(output);
            }
        }catch(Exception ex){};

        String queryString8 = "prefix ont: <http://ont.thomsonreuters.com/>\n" +
                "prefix md: <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/>\n" +
                "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "prefix go: <http://ld.test.data.thomsonreuters.com/>\n" +
                "SELECT ?quoteId ?isSuspended  ?value ?effectiveFrom ?effectiveTo ?effectiveFromNACode ?effectiveToNACode\n" +
                "\n" +
                "WHERE {\n" +
                "\t?x rdf:type <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/Quote> .\n" +
                "\t?x ont:permId ?quoteId .\n" +
                "\t?x md:isSuspended ?isSuspended .\n" +
                "\t?isSuspended rdf:value ?value .\n" +
                "\tOPTIONAL{?isSuspended md:effectiveFrom ?effectiveFrom} .\n" +
                "\tOPTIONAL{?isSuspended md:effectiveTo ?effectiveTo} .\n" +
                "\tOPTIONAL{?isSuspended md:effectiveFromNACode ?effectiveFromNACode} .\n" +
                "\tOPTIONAL{?isSuspended md:effectiveToNACode ?effectiveToNACode} .\n" +
                "}\n";

        Query query8 = QueryFactory.create(queryString8);
        try{
            QueryExecution qexec = QueryExecutionFactory.create(query8, model);
            ResultSet results = qexec.execSelect();
            for ( ; results.hasNext() ; )
            {
                //                System.out.println(soln.toString());


                QuerySolution soln = results.nextSolution() ;
                StringBuilder sb = new StringBuilder();
                sb.append(format(soln.get("quoteId").asNode())).append(';');

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
                multipleOutputs.write("IsSuspended", null, new Text(output.trim()));
                //context.write(key, new Text(output));
                //System.out.println(output);
            }
        }catch(Exception ex){};


        String queryString9 = "prefix ont: <http://ont.thomsonreuters.com/>\n" +
                "prefix md: <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/>\n" +
                "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "prefix go: <http://ld.test.data.thomsonreuters.com/>\n" +
                "SELECT ?quoteId ?expiryDate  ?value ?effectiveFrom ?effectiveTo ?effectiveFromNACode ?effectiveToNACode \n" +
                "\n" +
                "WHERE {\n" +
                "\t?x rdf:type <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/Quote> .\n" +
                "\t?x ont:permId ?quoteId .\n" +
                "\t?x md:expiryDate ?expiryDate .\n" +
                "\t?expiryDate rdf:value ?value .\n" +
                "\tOPTIONAL{?expiryDate md:effectiveFrom ?effectiveFrom} .\n" +
                "\tOPTIONAL{?expiryDate md:effectiveTo ?effectiveTo} .\n" +
                "\tOPTIONAL{?expiryDate md:effectiveFromNACode ?effectiveFromNACode} .\n" +
                "\tOPTIONAL{?expiryDate md:effectiveToNACode ?effectiveToNACode} .\n" +
                "}\n";

        Query query9 = QueryFactory.create(queryString9);
        try{
            QueryExecution qexec = QueryExecutionFactory.create(query9, model);
            ResultSet results = qexec.execSelect();
            for ( ; results.hasNext() ; )
            {
                //                System.out.println(soln.toString());


                QuerySolution soln = results.nextSolution() ;
                StringBuilder sb = new StringBuilder();
                sb.append(format(soln.get("quoteId").asNode())).append(';');

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
                multipleOutputs.write("ExpiryDate", null, new Text(output.trim()));
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



