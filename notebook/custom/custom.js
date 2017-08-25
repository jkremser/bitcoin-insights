var neighbors = function(graph, nodeId) {
  var k,
      neighbors = {},
      index = graph.allNeighborsIndex.get(nodeId).keyList() || {};

  for (k in index)
    neighbors[k] = graph.nodesIndex.get(index[k]);

  return neighbors;
};

// ugly
window.neighbors = neighbors;
