FROM eclipse-temurin:11

RUN apt-get update && \
    apt-get install --no-install-recommends -y wget && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir /jar
RUN wget -q https://dl.embulk.org/embulk-0.10.44.jar -O /jar/embulk.jar

RUN wget -q https://repo1.maven.org/maven2/org/jruby/jruby-complete/9.4.2.0/jruby-complete-9.4.2.0.jar -O /jar/jruby.jar

RUN wget -q https://repo1.maven.org/maven2/javax/xml/bind/jaxb-api/2.3.1/jaxb-api-2.3.1.jar -O /jar/jaxb-api.jar

WORKDIR /embulk

RUN mkdir /embulk/.embulk
COPY embulk.properties /embulk/.embulk/embulk.properties

RUN java -jar /jar/embulk.jar gem install msgpack bundler liquid

COPY Gemfile /embulk/Gemfile
COPY Gemfile.lock /embulk/Gemfile.lock
RUN java -jar /jar/embulk.jar bundle install --path .
