LOCAL_IMAGE=$(USER)/bitcoin-spark-notebook
GRAPH_DATA ?= $(HOME)/bitcoin/output/


.PHONY: build clean run test prettify open

build: clean prettify
	-echo -e "\n--- Copying graph data from: $(GRAPH_DATA) ---\n"
	cp -r $(GRAPH_DATA)/* .
	-sed -i'_bak' "s/\([[:space:]]*\"name\": \"Blockchain\".*\)/\1 \"data-info\": \""$${GRAPH_DATA#*-}"\",/g" blockchain.snb.ipynb
	# $(MAKE) -f $(THIS_FILE) prettify
	docker build -t $(LOCAL_IMAGE) .
	-rm -rf edges addresses blocks transactions
	-mv blockchain.snb.ipynb_bak blockchain.snb.ipynb

clean:
	-docker rmi -f $(LOCAL_IMAGE)

run:
	# remove the container to avoid the name colision
	-docker rm -f spark-notebook || true
	# remove the other notebooks (in 5 seconds)
	#-(sleep 5 && docker exec spark-notebook find /opt/docker/notebooks/ ! -path /opt/docker/notebooks/ -type d -exec rm -rf {} +)&
	# and run it
	docker run --name spark-notebook --rm -ti -p 9000:9000 $(LOCAL_IMAGE)

open:
	-(sleep 5.5 && firefox http://localhost:9000 &> /dev/null)&

prettify:
	cp ./blockchain.snb.ipynb ./blockchain.snb.ipynb_bak
	cat ./blockchain.snb.ipynb_bak | python -m json.tool > ./blockchain.snb.ipynb
	rm ./blockchain.snb.ipynb_bak
