LOCAL_IMAGE=$(USER)/bitcoin-notebook
RANDOM := $(shell bash -c 'echo $$$$')
TEST_CONTAINER="test-bitcoin-notebook-$(RANDOM)"
GRAPH_DATA=$(HOME)/bitcoin/output/*


.PHONY: build clean run test

build:
	cp -r $(GRAPH_DATA) .
	docker build -t $(LOCAL_IMAGE) .
	rm -rf edges nodes

clean:
	-docker rmi -f $(LOCAL_IMAGE)

run:
	docker run -ti -p 8888:8888 -e JUPYTER_NOTEBOOK_PASSWORD=dev $(LOCAL_IMAGE)
