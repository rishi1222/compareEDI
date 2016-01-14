package org.apache.jena.instrument;

/**
 * Created by rishikapoor on 11/01/2016.
 */
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.service.AbstractNodeTupleSplitWithNodesMapperWrapper;
import org.apache.jena.graph.Node;
import org.apache.jena.hadoop.rdf.types.QuadWritable;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by rishikapoor on 20/11/2015.
 */
public class InstMapper<TKey> extends AbstractNodeTupleSplitWithNodesMapperWrapper<TKey, Quad, QuadWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(InstMapper.class);

    private boolean tracing = false;
    private Text[] ns;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.tracing = LOG.isTraceEnabled();
    }

    @Override
    protected void map(TKey key, QuadWritable value, Context context) throws IOException,
            InterruptedException {
        context.write(new Text(format(value.get().getGraph())), value);
    }

    @Override
    protected Text[] split(QuadWritable tuple) {

        Quad q = tuple.get();
        StringBuilder sb = new StringBuilder();
        //System.out.println(q.toString());

        sb.append(format(q.getSubject()));
        sb.append(';');
        sb.append(format(q.getPredicate()));
        sb.append(';');
        sb.append(format(q.getObject()));
        sb.append(';');
        //System.out.println(sb.toString());
        return new Text[] { new Text(format(q.getGraph())), new Text(sb.toString()) };



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
        } else
        {
            String value[] = getUrl.split("/");
            URIVal = value[value.length-1];
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
        String lex = n.getLiteralLexicalForm() ;;

        return lex;
    }


    public String formatVar(Node n)         {
        return n.getName() ; }



}


