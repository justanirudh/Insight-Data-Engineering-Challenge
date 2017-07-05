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
    HashMap<String, List<Integer>> idToTTransactionsInNetwork;
    HashMap<String, List<String>> idToAllNeighbours;
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
                        if (idToPurchases.containsKey(purchase.id)) { //remove last purchase, add new one
                            List<Purchase> purchases = idToPurchases.get(purchase.id);
                            if (purchases.size() == transactionsSize) { //first remove
                                purchases.remove(transactionsSize - 1);
                            }
                            purchases.add(0, purchase);
                        } else {
                            List<Purchase> purchases = new ArrayList<>();
                            purchases.add(purchase);
                            idToPurchases.put(purchase.id, purchases);
                        }
                        break;
                    }
                    case "befriend": {
                        break;
                    }
                    case "unfriend": {
                        break;
                    }
                    default:
                        throw new IOException();
                }
                ++this.time;
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

            fileReader.close();
            //now process stream_log
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
