LOCAL_IMAGE=$(USER)/openshift-spark-with-data
GRAPH_DATA ?= $(HOME)/bitcoin/output/


.PHONY: build clean run test

build: clean
	-echo -e "\n--- Copying graph data from: $(GRAPH_DATA) ---\n"
	cp -r $(GRAPH_DATA)/{edges,nodes} .
	#echo $(GRAPH_DATA) >> ./nodes/data_from
	docker build -t $(LOCAL_IMAGE) .
	-rm -rf edges nodes

clean:
	-docker rmi -f $(LOCAL_IMAGE)

run:
	-docker rm -f spark-with-data || true
	docker run --name spark-with-data --rm -ti -p 9000:9000 $(LOCAL_IMAGE)
