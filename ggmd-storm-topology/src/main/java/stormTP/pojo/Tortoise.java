package stormTP.pojo;
import java.io.Serializable;

public class Tortoise implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private int id;

    private int top;

    private int tour;

    private int cellule;

    private int total;

    private int maxcel;

    public Tortoise() {}

    public Tortoise(final int id, final int top, final int tour, final int cellule, final int total, final int maxcel) {
        this.id = id;
        this.top = top;
        this.tour = tour;
        this.cellule = cellule;
        this.total = total;
        this.maxcel = maxcel;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getTop() {
        return top;
    }

    public void setTop(int top) {
        this.top = top;
    }

    public int getTour() {
        return tour;
    }

    public void setTour(int tour) {
        this.tour = tour;
    }

    public int getCellule() {
        return cellule;
    }

    public void setCellule(int cellule) {
        this.cellule = cellule;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getMaxcel() {
        return maxcel;
    }

    public void setMaxcel(int maxcel) {
        this.maxcel = maxcel;
    }
}
