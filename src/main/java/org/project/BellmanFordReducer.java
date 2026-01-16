package org.project;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BellmanFordReducer extends Reducer<Text, Vertex, Text, Text> {

    private static final int INF = Integer.MAX_VALUE;

    @Override
    public void reduce(Text vId, Iterable<Vertex> values, Context context)
            throws IOException, InterruptedException {

        Vertex v = new Vertex();
        v.setId(vId.toString());
        v.setDist(INF);
        for (Vertex value : values) {
            if(value.getDist() < v.getDist()) {
                v.setDist(value.getDist());
                v.setParentId(value.getParentId());
            }
            if (value.getNeighbors() != null && !value.getNeighbors().isEmpty()) {
                v.setNeighbors(value.getNeighbors());
            }
            if (value.isFlag()) {
                v.setFlag(true);
            }
        }
        context.write(new Text(v.getId()), new Text(v.argsToString()));
    }
}

