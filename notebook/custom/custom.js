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

require(['base/js/namespace']) {
    // setup 'ctrl-l' as shortcut for clearing current output
    Jupyter.keyboard_manager.command_shortcuts
           .add_shortcut('ctrl-shift-l', 'jupyter-notebook:clear-cell-output');
}
