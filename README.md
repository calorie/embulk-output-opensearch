# Elasticsearch output plugin for Embulk

**Notice** This plugin doesn't positively support [Amazon(AWS) Elasticsearch Service](https://aws.amazon.com/elasticsearch-service/).
Actually, AWS Elasticsearch Service supported AWS VPC at Oct 2017 and user is able to access to Es from EC2 instances in VPC subnet without any authentication.
You can use this plugin for AWS ES at your own risk.

- *[Amazon Elasticsearch Service Limits](http://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/aes-limits.html)*

## Overview

* **Plugin type**: output
* **Rollback supported**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **mode**: "insert" or "replace". See below(string, optional, default is insert)
- **nodes**: list of nodes. nodes are pairs of host and port (list, required)
  - NOTE: This plugin uses HTTP/REST Clients and uses TCP:9200 as a default. TCP:9300 is usually used for Transport Client.
- **use_ssl** Use SSL encryption (boolean, default is false)
- **auth_method** (string, default is 'none') 'none'/'basic'. See also [Authentication](#authentication).
- **user** Username for basic authentication (string, default is null)
- **password** Password for above user (string, default is null)
- ~~**cluster_name**: name of the cluster (string, default is "elasticsearch")~~ Not used now. May use in the future
- **index**: index name (string, required)
- **index_type**: index type (string, required)
- **id**: document id column (string, default is null)
- **bulk_actions**: Sets when to flush a new bulk request based on the number of actions currently added. (int, default is 1000)
- **bulk_size**: Sets when to flush a new bulk request based on the size of actions currently added. (long, default is 5242880)
- **fill_null_for_empty_column**: Fill null value when column value is empty (boolean, optional, default is false)
- ~~**concurrent_requests**: concurrent_requests (int, default is 5)~~  Not used now. May use in the future
- **maximum_retries** Number of maximam retry times (int, optional, default is 7)
- **initial_retry_interval_millis** Initial interval between retries in milliseconds (int, optional, default is 1000)
- **maximum_retry_interval_millis** Maximum interval between retries in milliseconds (int, optional, default is 120000)
- **timeout_millis** timeout in milliseconds for each HTTP request(int, optional, default is 60000)
- **connect_timeout_millis** connection timeout in milliseconds for HTTP client(int, optional, default is 60000)
- **max_snapshot_waiting_secs** maximam waiting time in second when snapshot is just creating before delete index. works when `mode: replace` (int, optional, default is 1800)
### Modes

#### insert:

default.
This mode writes data to existing index.

#### replace:

1. Create new temporary index 
2. Insert data into the new index
3. replace the alias with the new index. If alias doesn't exists, plugin will create new alias.
4. Delete existing (old) index if exists

Index should not exists with the same name as the alias

```yaml
out:
  type: elasticsearch
  mode: replace
  nodes:
  - {host: localhost, port: 9200}
  index: <alias name> # plugin generates index name like <index>_%Y%m%d-%H%M%S 
  index_type: <index type>
```

### Authentication

This plugin supports Basic authentication and works with [Elastic Cloud](https://cloud.elastic.co/) and 'Security'(formally Sield).
'Security' also supports LDAP and Active Directory. This plugin doesn't supports these auth methods.

```yaml
use_ssl: true
auth_method: basic
user: <username>
password: <password>
```

## Example

```yaml
out:
  type: elasticsearch
  mode: insert
  nodes:
  - {host: localhost, port: 9200}
  index: <index name>
  index_type: <index type>
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
$ ./gradlew bintrayUpload # release embulk-output-elasticsearch to Bintray maven repo
```

## Test

```
$ ./gradlew test  # -t to watch change of files and rebuild continuously
```

To run unit tests, we need to configure the following environment variables.

When environment variables are not set, skip almost test cases.

```
ES_HOST
ES_PORT(optional, if needed, default: 9300)
ES_INDEX
ES_INDEX_TYPE
```

If you're using Mac OS X El Capitan and GUI Applications(IDE), like as follows.
```
$ vi ~/Library/LaunchAgents/environment.plist
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>my.startup</string>
  <key>ProgramArguments</key>
  <array>
    <string>sh</string>
    <string>-c</string>
    <string>
      launchctl setenv ES_HOST example.com
      launchctl setenv ES_PORT 9300
      launchctl setenv ES_INDEX embulk
      launchctl setenv ES_INDEX_TYPE embulk
    </string>
  </array>
  <key>RunAtLoad</key>
  <true/>
</dict>
</plist>

$ launchctl load ~/Library/LaunchAgents/environment.plist
$ launchctl getenv ES_INDEX //try to get value.

Then start your applications.
```
