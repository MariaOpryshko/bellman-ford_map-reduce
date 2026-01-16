package org.project;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class Vertex implements Writable {
    private String id = "";
    private int dist = Integer.MAX_VALUE;
    private String parentId = "";
    private Map<String, Integer> neighbors = new HashMap<>();
    private boolean flag =  false;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id == null ? "" : id);
        out.writeInt(dist);
        out.writeUTF(parentId == null ? "" : parentId);

        if (neighbors == null) {
            out.writeInt(0);
        } else {
            out.writeInt(neighbors.size());
            for (Map.Entry<String, Integer> e : neighbors.entrySet()) {
                out.writeUTF(e.getKey());
                out.writeInt(e.getValue());
            }
        }
        out.writeBoolean(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readUTF();
        dist = in.readInt();
        parentId = in.readUTF();

        int size = in.readInt();
        neighbors = new HashMap<>();
        for (int i = 0; i < size; i++) {
            neighbors.put(in.readUTF(), in.readInt());
        }
        flag = in.readBoolean();
    }

    public String argsToString() {
        StringBuilder sb = new StringBuilder();
        
        sb.append("{");
        java.util.Iterator<Map.Entry<String, Integer>> it = neighbors.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Integer> e = it.next();
            sb.append(e.getKey()).append("=").append(e.getValue());
            if (it.hasNext()) sb.append(",");
        }
        sb.append("}");
        
        sb.append("\t");
        sb.append(dist);
        
        sb.append("\t");
        sb.append(parentId);

        sb.append("\t");
        sb.append(flag);

        return sb.toString();
    }

    public void setNeighborsFromStr(String input) {
        if (input == null || input.length() < 2) {
            this.neighbors = new HashMap<>();
            return;
        }
        Map<String, Integer> map = new HashMap<>();
        // убираем фигурные скобки
        input = input.substring(1, input.length() - 1);
        
        if (input.isEmpty()) {
            this.neighbors = map;
            return;
        }
        
        String[] entries = input.split(",");

        for (String entry : entries) {
            String[] parts = entry.split("=");
            String key = parts[0];
            Integer value = Integer.parseInt(parts[1]);
            map.put(key, value);
        }
        this.neighbors = map;
    }
}
