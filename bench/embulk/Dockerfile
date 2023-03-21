FROM eclipse-temurin:11

ARG PACKAGE_USERNAME
ARG PACKAGE_TOKEN

ENV PACKAGE_USERNAME=${PACKAGE_USERNAME}
ENV PACKAGE_TOKEN=${PACKAGE_TOKEN}

RUN apt-get update && \
    apt-get install --no-install-recommends -y wget netbase && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir /jar && \
  wget -q https://dl.embulk.org/embulk-0.10.44.jar -O /jar/embulk.jar && \
  wget -q https://repo1.maven.org/maven2/org/jruby/jruby-complete/9.4.2.0/jruby-complete-9.4.2.0.jar -O /jar/jruby.jar && \
  wget -q https://repo1.maven.org/maven2/javax/xml/bind/jaxb-api/2.3.1/jaxb-api-2.3.1.jar -O /jar/jaxb-api.jar

WORKDIR /embulk

RUN mkdir /embulk/.embulk
COPY embulk.properties /embulk/.embulk/embulk.properties

COPY Gemfile /embulk/Gemfile
COPY Gemfile.lock /embulk/Gemfile.lock

RUN java -jar /jar/embulk.jar gem install msgpack bundler liquid && \
  java -jar /jar/embulk.jar bundle config --global rubygems.pkg.github.com $PACKAGE_USERNAME:$PACKAGE_TOKEN && \
  java -jar /jar/embulk.jar bundle install --path .
