package org.project;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.project.Vertex;

import java.io.IOException;
import java.util.Map;

public class BellmanFordMapper extends Mapper<Object, Text, Text, Vertex> {
    private static final int INF = Integer.MAX_VALUE;

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");

        Vertex v = new Vertex();
        if (parts.length == 2) { // first iteration of initializing distances
            v.setId(parts[0]);
            v.setNeighborsFromStr(parts[1]);
            String sourceVertex = context.getConfiguration().get("source.vertex.id");
            if (sourceVertex.equals(parts[0])) {
                v.setDist(0);
                for (Map.Entry<String, Integer> entry : v.getNeighbors().entrySet()) {
                    String neighbor = entry.getKey();

                    Vertex neighborVertex = new Vertex();
                    neighborVertex.setId(neighbor);
                    neighborVertex.setDist(v.getDist() + entry.getValue());
                    neighborVertex.setParentId(v.getId());
                    neighborVertex.setFlag(true);
                    context.write(new Text(neighborVertex.getId()), neighborVertex);
                }
            } else {
                v.setDist(INF);
            }
            context.write(new Text(v.getId()), v);
        } else if (parts.length == 5) { //format: <vertex id> <neighbors and weights> <parent vertex id> <cur min dist> <flag>
            v.setId(parts[0]);
            v.setNeighborsFromStr(parts[1]);
            v.setParentId(parts[3]);
            v.setDist(Integer.parseInt(parts[2]));
            v.setFlag(Boolean.parseBoolean(parts[4]));
            if (v.isFlag()) {
                v.setFlag(false);
                for (Map.Entry<String, Integer> entry : v.getNeighbors().entrySet()) {
                    String neighbor = entry.getKey();
                    Integer weight = entry.getValue();

                    Vertex neighborVertex = new Vertex();
                    neighborVertex.setId(neighbor);
                    neighborVertex.setDist(v.getDist() + weight);
                    neighborVertex.setParentId(v.getId());
                    neighborVertex.setFlag(true);
                    context.write(new Text(neighborVertex.getId()), neighborVertex);
                }
            }
            context.write(new Text(v.getId()), v);
        }
    }
}
