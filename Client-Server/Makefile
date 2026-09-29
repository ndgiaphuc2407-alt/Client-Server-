
default: animate_server animate_client

libanimate:
	@if [ ! -d $@ ]; then \
		echo "ERROR: libanimate not found" > /dev/stderr; \
		echo "Download and unzip libanimate.zip from P2 resources" > /dev/stderr; \
		false; \
	fi


animate_server: animate_server.c | libanimate
	gcc $^ -I libanimate/include -Llibanimate/lib -lanimate -pthread -o $@

animate_client: animate_client.c
	gcc $^ -o $@


