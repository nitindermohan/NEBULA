package de.tum.i11.bcsim.graph;

import java.util.ArrayList;
import java.util.List;

public interface GraphStrategy {
    ArrayList<List<Edge>> getEdges();
    int getNodes();
    double getMaxLatency();
    double getBandWidth();
}
