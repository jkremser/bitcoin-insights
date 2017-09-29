LOCAL_IMAGE=$(USER)/bitcoin-notebook
GRAPH_DATA ?= $(HOME)/bitcoin/output/


.PHONY: build clean run test

build:
	-echo -e "\n--- Copying graph data from: $(GRAPH_DATA) ---\n"
	cp -r $(GRAPH_DATA)/{edges,nodes} .
	echo $(GRAPH_DATA) >> ./nodes/data_from
	docker build -t $(LOCAL_IMAGE) .
	-rm -rf edges nodes

clean:
	-docker rmi -f $(LOCAL_IMAGE)

run:
	docker run --rm -ti -p 8888:8888 $(LOCAL_IMAGE)
