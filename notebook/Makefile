LOCAL_IMAGE=$(USER)/bitcoin-notebook
GRAPH_DATA1 ?= $(HOME)/bitcoin/output/
GRAPH_DATA2 ?= $(HOME)/bitcoin/output/
#GRAPH_DATA3 ?= $(HOME)/bitcoin/output/


.PHONY: build clean run test

build: clean
	-echo -e "\n--- Copying graph data from: $(GRAPH_DATA1) ---\n"
	-echo -e "\n--- Copying graph data from: $(GRAPH_DATA2) ---\n"
#	-echo -e "\n--- Copying graph data from: $(GRAPH_DATA3) ---\n"
	cp -r $(GRAPH_DATA1)/edges edges1
	cp -r $(GRAPH_DATA1)/nodes nodes1
	cp -r $(GRAPH_DATA2)/edges edges2
	cp -r $(GRAPH_DATA2)/nodes nodes2
#	cp -r $(GRAPH_DATA3)/edges edges3
#	cp -r $(GRAPH_DATA3)/nodes nodes3
	docker build -t $(LOCAL_IMAGE) .
	-rm -rf edges* nodes*

clean:
	-docker rmi -f $(LOCAL_IMAGE)

run:
	docker run --rm -ti -p 8888:8888 $(LOCAL_IMAGE)

open:
	-(sleep 2 && xdg-open http://localhost:8888 &> /dev/null)&

