LOCAL_IMAGE=$(USER)/bitcoin-notebook
RANDOM := $(shell bash -c 'echo $$$$')
TEST_CONTAINER="test-bitcoin-notebook-$(RANDOM)"


.PHONY: build clean run test

build:
	cp -r ../parquet-converter/data/example1/output/* .
	docker build -t $(LOCAL_IMAGE) .
	rm *.parquet

clean:
	-docker rmi -f $(LOCAL_IMAGE)

run:
	docker run -ti -p 8888:8888 -e JUPYTER_NOTEBOOK_PASSWORD=dev $(LOCAL_IMAGE)
