package org.apache.jena.service;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.jena.hadoop.rdf.types.AbstractNodeTupleWritable;
import org.apache.jena.hadoop.rdf.types.NodeWritable;
import org.apache.jena.hadoop.rdf.types.QuadWritable;

import java.io.IOException;

/**
 * Created by rishikapoor on 20/11/2015.
 */
public abstract class AbstractNodeTupleSplitWithNodesMapperWrapper<TKey, TValue, T extends AbstractNodeTupleWritable<TValue>> extends
        Mapper<TKey, T, Text, QuadWritable> {

//    @Override
//    protected void map(TKey key, T value, Context context) throws IOException, InterruptedException {
//        Text[] ns = this.split(value);
//        for (Text n : ns) {
//            context.write(n, n);
//        }
//    }

    /**
     * Splits the node tuple type into the individual nodes
     *
     * @param tuple
     *            Tuple
     * @return Nodes
     */
    protected abstract Text[] split(T tuple);
}

