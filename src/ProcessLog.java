import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by paanir on 7/4/17.
 */
public class ProcessLog {

    HashMap<String, List<Purchase>> idToPurchases; //ID to purchases map. Length of purchases is max T
    HashMap<String, GraphNode> idToNeighboursGraph; //the graph
    HashMap<String, List<String>> idToAllNeighbours;
    HashMap<String, List<Integer>> idToTTransactionsInNetwork;

    int degree;
    int transactionsSize;
    int time;

    public void setupInitialState(BufferedReader bufferedReader, ObjectMapper mapper) {
        try {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                HashMap map = mapper.readValue(new StringReader(line), HashMap.class);
                String eventType = (String) map.get("event_type");
                switch (eventType) {
                    case "purchase": { //add recent T purchases to each id
                        Purchase purchase = mapper.readValue(line, Purchase.class);
                        purchase.time = this.time;
                        String id = purchase.id;
                        if (idToPurchases.containsKey(id)) { //remove last purchase, add new one
                            List<Purchase> purchases = idToPurchases.get(id);
                            if (purchases.size() == transactionsSize) { //first remove
                                purchases.remove(transactionsSize - 1);
                            }
                            purchases.add(0, purchase);
                        } else {
                            List<Purchase> purchases = new ArrayList<>();
                            purchases.add(purchase);
                            idToPurchases.put(id, purchases);
                        }
                        ++this.time;
                        break;
                    }
                    case "befriend": {
                        Befriend befriend = mapper.readValue(line, Befriend.class);
                        String id1 = befriend.id1;
                        GraphNode node1 = new GraphNode(id1);
                        String id2 = befriend.id2;
                        GraphNode node2 = new GraphNode(id2);
                        if(!idToNeighboursGraph.containsKey(id1))
                            idToNeighboursGraph.put(id1, node1);
                        if(!idToNeighboursGraph.containsKey(id2))
                            idToNeighboursGraph.put(id2, node2);
                        node1.neighbors.add(id2);
                        node2.neighbors.add(id1);
                        break;
                    }
                    case "unfriend": {
                        Unfriend unfriend = mapper.readValue(line, Unfriend.class);
                        String id1 = unfriend.id1;
                        String id2 = unfriend.id2;
                        idToNeighboursGraph.get(id1).neighbors.remove(id2);
                        idToNeighboursGraph.get(id2).neighbors.remove(id1);
                        break;
                    }
                    default:
                        throw new IOException();
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        try {
            //first process batch_log
            File file = new File("/Users/paanir/InsightDataEngineering/anomaly_detection/log_input/batch_log.json");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            ObjectMapper mapper = new ObjectMapper();

            String line = bufferedReader.readLine();

            Parameters params = mapper.readValue(line, Parameters.class);

            ProcessLog pl = new ProcessLog();

            pl.time = 0;
            pl.idToPurchases = new HashMap<>();
            pl.idToNeighboursGraph = new HashMap<>();
            pl.idToTTransactionsInNetwork = new HashMap<>();
            pl.idToAllNeighbours = new HashMap<>();
            pl.degree = Integer.parseInt(params.degree);
            pl.transactionsSize = Integer.parseInt(params.transactions);

            pl.setupInitialState(bufferedReader, mapper);

//            for(String key : pl.idToNeighboursGraph.keySet()){
//                System.out.print(key + " -> ");
//                for(String n : pl.idToNeighboursGraph.get(key).neighbors)
//                    System.out.print( n +",");
//                System.out.println();
//            }

            fileReader.close();
            //now process stream_log
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
