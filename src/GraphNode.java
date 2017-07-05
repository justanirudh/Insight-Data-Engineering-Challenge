import java.util.ArrayList;

/**
 * Created by paanir on 7/4/17.
 */
public class GraphNode {
    String id;
    String parentString = "";
    String color = "white";
    Integer distSrc = -1;
    ArrayList<String> neighbors = new ArrayList<>();

    public GraphNode(String id) {
        this.id = id;
    }
}
