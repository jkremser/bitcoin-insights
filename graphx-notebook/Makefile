LOCAL_IMAGE=$(USER)/bitcoin-spark-notebook
RANDOM := $(shell bash -c 'echo $$$$')
TEST_CONTAINER="test-bitcoin-spark-notebook-$(RANDOM)"
GRAPH_DATA ?= $(HOME)/bitcoin/output/*


.PHONY: build clean run test

build:
	-echo -e "\n--- Copying graph data from: $(GRAPH_DATA) ---\n"
	-cp -r $(GRAPH_DATA) .
	docker build -t $(LOCAL_IMAGE) .
	-rm -rf edges nodes

clean:
	-docker rmi -f $(LOCAL_IMAGE)

run:
	-docker rm -f spark-notebook || true
	-(sleep 5 && docker exec spark-notebook find /opt/docker/notebooks/ ! -path /opt/docker/notebooks/ -type d -exec rm -rf {} +)&
	docker run --name spark-notebook --rm -ti -p 9000:9000 $(LOCAL_IMAGE)
