package stormTP.pojo;
import java.io.Serializable;

public class FilteredTortoise implements Serializable {
    private static final long serialVersionUID = 1L;

    private int id;
    private int top;
    private String nom;
    private int nbCellsParcouru;
    private int total;
    private int maxcel;

    public FilteredTortoise() {}

    public FilteredTortoise(int id, int top, String nom, int nbCellsParcouru, int total, int maxcel) {
        this.id = id;
        this.top = top;
        this.nom = nom;
        this.nbCellsParcouru = nbCellsParcouru;
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

    public String getNom() {
        return nom;
    }

    public void setNom(String nom) {
        this.nom = nom;
    }

    public int getNbCellsParcouru() {
        return nbCellsParcouru;
    }

    public void setNbCellsParcouru(int nbCellsParcouru) {
        this.nbCellsParcouru = nbCellsParcouru;
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
