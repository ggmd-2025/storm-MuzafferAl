package stormTP.pojo;

import java.util.List;

public class RaceData {
    
    private long timestamp;
    private List<Tortoise> runners;

    public long getTimestamp() { 
        return timestamp; 
    }

    public void setTimestamp(long timestamp) { 
        this.timestamp = timestamp; 
    }

    public List<Tortoise> getRunners() {
        return runners; 
    }

    public void setRunners(List<Tortoise> runners) {
        this.runners = runners; 
    }

}
