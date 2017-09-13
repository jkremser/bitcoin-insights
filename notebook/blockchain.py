
# coding: utf-8

# # Blockchain analysis
# 
# In case you know nothing about the Bitcoin and Blockchain, you can start by watching the following video.

# In[ ]:


from IPython.display import HTML
HTML('<iframe width="750" height="430" src="https://www.youtube.com/embed/Lx9zgZCMqXE?rel=0&amp;controls=1&amp;showinfo=0" frameborder="0" allowfullscreen></iframe>')


# ## Basic setup
# 
# Here we will import the `pyspark` module and set up a `SparkSession`. By default, we'll use a `SparkSession` running locally, with one Spark executor; we're dealing with small data, so it doesn't make sense to run against a cluster, but the `local[1]` can be changed with the ip of the Spark cluster.
# 

# In[1]:


import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import regexp_replace

spark = SparkSession.builder                     .master("local[4]")                     .config("spark.driver.memory", "4g")                     .getOrCreate()

sc = spark.sparkContext


# ## Loading the data
# 
# To obtain the graph representing the transaction in the Bitcoin network, we need to load set of nodes representing the wallets (fingerprints of the public keys) and the set of edges representing each transaction. For this example we will use two parquet files that were generated from the blockchain data by this [convertor](https://github.com/Jiri-Kremser/bitcoin-insights/tree/master/parquet-converter).

# In[2]:


raw_nodes = spark.read.load("/data/nodes.parquet")                       .withColumnRenamed("_1", "id")                       .withColumnRenamed("_2", "Wallet")
raw_nodes.show(5)


# As you can see, each record in the wallet column contains a string `bitcoinaddress_<hash>`, where the hash is the actual address of the wallet. Let's remove the redundant prefix.

# In[3]:


nodes = raw_nodes.withColumn("Wallet", regexp_replace("Wallet", "bitcoinaddress_", "")).cache()
nodes.show(5)


# As you can see, each record in the wallet column contains a string `bitcoinaddress_<hash>`, where the hash is the actual address of the wallet. Let's remove the redundant prefix.

# We can also verify, that these addresses are real on https://blockchain.info/address/. 
# 
# Example:
#  * get random address

# In[4]:


random_address = nodes.rdd.takeSample(False, 1)[0][1]
random_address


#  * create the link from the address: https://blockchain.info/address/{{random_address}} 
#  
#  (todo: http://jupyter-contrib-nbextensions.readthedocs.io/en/latest/nbextensions/python-markdown/readme.html)

# In[5]:


edges = spark.read.load("/data/edges.parquet")                   .withColumnRenamed("srcId", "src")                   .withColumnRenamed("dstId", "dst")                   .cache()
edges.show(5)
edges.count()


# ## Constructing the graph representation
# 
# Spark contains API for graph processing. It's called [graphx](https://spark.apache.org/graphx/) and it also comes with multiple built-in algorithms like page-rank. It uses the [Pregel API](https://spark.apache.org/docs/latest/graphx-programming-guide.html#pregel-api).

# In[6]:


from graphframes import *

g = GraphFrame(nodes, edges).cache()


# #### Get the top 10 wallets with respect to the transaction count
# 
# First, by sorting the nodes by `inDegree` which corresponds to the number of transactions received.

# In[7]:


vertexInDegrees = g.inDegrees
vertexInDegrees.join(nodes, vertexInDegrees.id == nodes.id)                .drop("id")                .orderBy("inDegree", ascending=False)                .take(10)


# Then by using the `outDegree` ~ # transactions sent

# In[8]:


vertexOutDegrees = g.outDegrees
senders = vertexOutDegrees.join(nodes, vertexOutDegrees.id == nodes.id)                           .drop("id")                           .orderBy("outDegree", ascending=False)
senders.take(10)


# You can verify on blockchain.info that the actual number of transaction is lower than what we have just calculated. This stems from the fact how Bitcoin works, when sending BTC from a wallet to another one, it actually sends all the BTC and the receiving node will sends back the rest. We are oversimplyfying here and you can find the details [here](https://en.bitcoin.it/wiki/Transaction) or [here](https://bitcoin.stackexchange.com/questions/9007/why-are-there-two-transaction-outputs-when-sending-to-one-address).

# #### Find circles of length 2

# In[30]:


motifs = g.find("(a)-[]->(b); (b)-[]->(a)")
motifs.count()
# motifs.show(5)


# #### Resource consuming foo

# In[10]:


# this fails with OOM error
#results = g.pageRank(resetProbability=0.15, maxIter=1)
#results.vertices.select("id", "pagerank").show()
#results.edges.select("src", "dst", "weight").show()

# g.labelPropagation(maxIter)

# this would be nice display(ranks.vertices.orderBy(ranks.vertices.pagerank.desc()).limit(20))


# ## Visualization of a sub-graph
# 
# Our data contain a lot of transactions (2 087 249 transactions among 546 651 wallets) so let's show only a small fraction of the transaction graph. We will show all the outgoing transaction of particular bitcoin address.

# In[86]:


from pyspark.sql.functions import col
import random

# feel free to use any address that is present in the dataset
address = senders.take(1000)[999].Wallet

sub_graph = g.find("(src)-[e]->(dst)")              .filter(col('src.Wallet') == address)
    
def node_to_dict(r):
    return {
        'id': r[0],
        'label': r[1],
        'x': random.uniform(0,1),
        'y': random.uniform(0,1),
        'size': random.uniform(0.2,1)
    }

sub_nodes = sub_graph.select("dst.id", "dst.Wallet").distinct()
sub_edges = sub_graph.select("e.src", "e.dst")

target_nodes_dict = map(node_to_dict, sub_nodes.collect())

def edge_to_dict(i, r):
    return {
        'id': i,
        'source': r[0],
        'target': r[1]
    }

sub_edges_dict = [edge_to_dict(i, r) for i, r in enumerate(sub_edges.collect())]

target_nodes_dict.append({
    'id': sub_edges.first()['src'],
    'label': address,
    'color': '#999',
    'x': -1,
    'y': 0.5,
    'size': 2
})


# Now we are ready to show the data using the [sigmajs](sigmajs.org) library.

# In[65]:


get_ipython().run_cell_magic(u'javascript', u'', u"require.config({\n    paths: {\n        sigmajs: 'https://cdnjs.cloudflare.com/ajax/libs/sigma.js/1.2.0/sigma.min'\n    }\n});\n\nrequire(['sigmajs']);")


# In[87]:


from IPython.core.display import display, HTML
from string import Template
import json

js_text_template = Template(open('js/sigma-graph.js','r').read())

graph_data = { 'nodes': target_nodes_dict, 'edges': sub_edges_dict }

js_text = js_text_template.substitute({'graph_data': json.dumps(graph_data),
                                       'container': 'graph-div'})

html_template = Template('''
<div id="graph-div" style="height:400px"></div>
<script> $js_text </script>
''')

HTML(html_template.substitute({'js_text': js_text}))


# In[106]:


sub_g = GraphFrame(sub_nodes.union(sub_graph.select("src.id", "src.Wallet").distinct()), sub_edges).cache()
sub_g

# #results = g.pageRank(resetProbability=0.15, maxIter=1)
# #results.vertices.select("id", "pagerank").show()
# #results.edges.select("src", "dst", "weight").show()
# # g.labelPropagation(maxIter)

# # this would be nice display(ranks.vertices.orderBy(ranks.vertices.pagerank.desc()).limit(20))


results = sub_g.pageRank(resetProbability=0.15, maxIter=3)


# In[109]:


results.vertices.orderBy(results.vertices.pagerank.asc()).limit(20).show()
results.edges.select("src", "dst", "weight").show()


# In[ ]:


labeled = g.labelPropagation(maxIter=3)
g.show()


# In[ ]:


g.show()

