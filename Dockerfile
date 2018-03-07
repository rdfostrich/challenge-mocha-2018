FROM node:latest

# Install Java 8, based on https://github.com/docker-library/openjdk/blob/2598f7123fce9ea870e67f8f9df745b2b49866c0/8-jdk/Dockerfile
RUN apt-get update && apt-get install -y --no-install-recommends \
		bzip2 \
		unzip \
		xz-utils \
	&& rm -rf /var/lib/apt/lists/*
ENV LANG C.UTF-8
RUN ln -svT "/usr/lib/jvm/java-8-openjdk-$(dpkg --print-architecture)" /docker-java-home
ENV JAVA_HOME /docker-java-home
ENV JAVA_VERSION 8u151
ENV JAVA_DEBIAN_VERSION 8u151-b12-1~deb9u1
ENV CA_CERTIFICATES_JAVA_VERSION 20170531+nmu1
RUN set -ex; \
	\
# deal with slim variants not having man page directories (which causes "update-alternatives" to fail)
	if [ ! -d /usr/share/man/man1 ]; then \
		mkdir -p /usr/share/man/man1; \
	fi; \
	\
	apt-get update; \
	apt-get install -y \
		openjdk-8-jdk="$JAVA_DEBIAN_VERSION" \
		ca-certificates-java="$CA_CERTIFICATES_JAVA_VERSION" \
	; \
	rm -rf /var/lib/apt/lists/*; \
	\
# verify that "docker-java-home" returns what we expect
	[ "$(readlink -f "$JAVA_HOME")" = "$(docker-java-home)" ]; \
	\
# update-alternatives so that future installs of other OpenJDK versions don't change /usr/bin/java
	update-alternatives --get-selections | awk -v home="$(readlink -f "$JAVA_HOME")" 'index($3, home) == 1 { $2 = "manual"; print | "update-alternatives --set-selections" }'; \
# ... and verify that it actually worked for one of the alternatives we care about
	update-alternatives --query java | grep -q 'Status: manual'
RUN /var/lib/dpkg/info/ca-certificates-java.postinst configure

# Install requirements for OSTRICH ingester
ADD install-kc.sh install-kc.sh
ENV CC clang
ENV CXX clang++
RUN apt-get update && \
    apt-get install -y clang && \
    curl -sSL https://cmake.org/files/v3.5/cmake-3.5.2-Linux-x86_64.tar.gz | tar -xzC /opt && \
    ./install-kc.sh
ENV PATH="/opt/cmake-3.5.2-Linux-x86_64/bin/:$PATH"

# Add OSTRICH ingestion script and install
ADD ingester/* /ostrich/ingester/
RUN export cmake="/opt/bin/cmake" && cd /ostrich/ingester && npm install --unsafe-perm

# Fetch comunica-ostrich from GitHub
RUN git clone -n https://github.com/comunica/comunica.git /ostrich/comunica && cd /ostrich/comunica && git checkout b03d1735ddd5e24bcad15d036e746f645f330af7
RUN cd /ostrich/comunica && npm install

# Add pre-compiled HOBBIT system adapter
ADD build/libs/OstrichHobbitMochaVersioningAdapter-1.0.0-all.jar /ostrich/ostrich-1.0.0.jar

WORKDIR /ostrich

CMD java -cp example.jar org.hobbit.core.run.ComponentStarter org.rdfostrich.hobbit.mocha.versioning.OstrichSystemAdapter
