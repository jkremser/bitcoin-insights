LOCAL_IMAGE=$(USER)/openshift-spark-with-data
GRAPH_DATA ?= $(HOME)/bitcoin/output/
GRAPH_DATA1 ?= $(HOME)/bitcoin/output/
GRAPH_DATA2 ?= $(HOME)/bitcoin/output/
GRAPH_DATA3 ?= $(HOME)/bitcoin/output/


.PHONY: build clean run test

build: clean
	-echo -e "\n--- Copying graph data from: $(GRAPH_DATA) ---\n"
	cp -r $(GRAPH_DATA)/{edges,nodes} .
	#echo $(GRAPH_DATA) >> ./nodes/data_from
	docker build -t $(LOCAL_IMAGE) .
	-rm -rf edges nodes

build-3-graphs: clean
	-echo -e "\n--- Copying graph data from: $(GRAPH_DATA1) ---\n"
	-echo -e "\n--- Copying graph data from: $(GRAPH_DATA2) ---\n"
	-echo -e "\n--- Copying graph data from: $(GRAPH_DATA3) ---\n"
	cp -r $(GRAPH_DATA1)/edges edges1
	cp -r $(GRAPH_DATA1)/nodes nodes1
	cp -r $(GRAPH_DATA2)/edges edges2
	cp -r $(GRAPH_DATA2)/nodes nodes2
	cp -r $(GRAPH_DATA3)/edges edges3
	cp -r $(GRAPH_DATA3)/nodes nodes3
	docker build -f ./Dockerfile_3_graphs -t $(LOCAL_IMAGE) .
	-rm -rf edges* nodes*

clean:
	-docker rmi -f $(LOCAL_IMAGE)

run:
	-docker rm -f spark-with-data || true
	docker run --name spark-with-data --rm -ti -p 9000:9000 $(LOCAL_IMAGE)
