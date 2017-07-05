import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by paanir on 7/4/17.
 */
public class Parameters {
    @JsonProperty("D")
    public String degree;

    @JsonProperty("T")
    public String transactions;
}

