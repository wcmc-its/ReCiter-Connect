package reciter.connect.vivo.sdb;

public enum VivoGraphs {

    PEOPLE_GRAPH("http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople"),
    OFA_GRAPH("http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa"),
    INFOED_GRAPH("http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus"),
    PUBLICATIONS_GRAPH("http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications"),
    VITRO_KB_INF_GRAPH("http://vitro.mannlib.cornell.edu/default/vitro-kb-inf"),
    DEFAULT_KB_2_GRAPH("http://vitro.mannlib.cornell.edu/default/vitro-kb-2");

    private String value;

    VivoGraphs(final String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return this.getValue();
    }
    
}