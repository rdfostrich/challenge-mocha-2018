FROM node:8

# Install Java 8
RUN echo 'deb http://ftp.debian.org/debian jessie-backports main' >> /etc/apt/sources.list
RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y -t jessie-backports openjdk-8-jre-headless

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
RUN cd /ostrich/comunica && npm install --unsafe-perm

# Add pre-compiled HOBBIT system adapter
ADD build/libs/OstrichHobbitMochaVersioningAdapter-1.0.0-all.jar /ostrich/ostrich-1.0.0.jar

WORKDIR /ostrich

CMD java -cp ostrich-1.0.0.jar org.hobbit.core.run.ComponentStarter org.rdfostrich.hobbit.mocha.versioning.OstrichSystemAdapter
