LOCAL_IMAGE=$(USER)/bitcoin-notebook
GRAPH_DATA ?= $(HOME)/bitcoin/output/


.PHONY: build clean run test

build: clean
	-echo -e "\n--- Copying graph data from: $(GRAPH_DATA) ---\n"
	cp -r $(GRAPH_DATA)/edges edges
	cp -r $(GRAPH_DATA)/addresses addresses
	cp -r $(GRAPH_DATA)/blocks blocks
	cp -r $(GRAPH_DATA)/transactions transactions
#	cp -r $(GRAPH_DATA)/edges edges3
#	cp -r $(GRAPH_DATA)/nodes nodes3
	docker build -t $(LOCAL_IMAGE) .
	-rm -rf edges* addresses* blocks* transactions*

clean:
	-docker rmi -f $(LOCAL_IMAGE)

run:
	docker run --rm -ti -p 8888:8888 $(LOCAL_IMAGE)

open:
	-(sleep 2 && xdg-open http://localhost:8888 &> /dev/null)&

