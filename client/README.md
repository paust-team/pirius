# ShapleQ Client
If you want ShapleQ Client only, just type `go get github.com/paust-team/ShapleQ/client`
## Usage
Before running client cli, ShapleQ broker and zookeeper must be running

### Create topic
- **Flags**
	- `-z` zk-address `required`
	- `—n` topic name `required`
	- `—m` topic meta or description
```
shapelq-cli topic create -z [zk-host] -n [topic-name] -m [topic-description]
```

### Delete topic
- **Flags**
	- `-z` zk-address `required`
	- `—n` topic name `required`
```
shapelq-cli topic delete -z [zk-host] -n [topic-name]
```

### List topic
- **Flags**
	- `-z` zk-address `required`
```
shapelq-cli topic list -z [zk-host]
```

### Describe topic
- **Flags**
	- `-z` zk-address `required`
	- `—n` topic name `required`
```
shapelq-cli topic describe -z [zk-host] -n [topic-name]
```

### Publish topic data
***NOTE: Before publish to topic, topic must be created(see Create topic data cmd)***
- **Flags**
	- `-p` broker port (default 11010)
	- `-z` zk-address `required`
	- `—n` topic name `required`
	- `—f` file path to publish (read from file and publish data line by)
	
```
shapelq-cli publish [byte-string-data-to-publish] -n [topic-name] -z [zk-host]
```

### Subscribe topic data
***NOTE: Before subscribe the topic, at least one data must be published)***
- **Flags**
	- `-p` broker port (default 11010)
	- `-z` zk-address `required`
	- `—n` topic name `required`
	- `—o` start offset (default 0)
```
shapelq-cli subscribe -n [topic-name] -z [zk-host]
```
Subscribe command will not stop until broker had stopped or received `ctrl+c` from user


## Development Guide
### Producer
### Consumer
