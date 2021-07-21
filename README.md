# MediaCloud to Kafka

This project implements a system for writing [MediaCloud](https://github.com/mediacloud/api-client) stories to Kafka.

## Usage

```
docker build . -t mediacloud

docker run -it --rm mediacloud ./run.py\
           --mediacloud-key <mediacloud_key>\
           -b <kafka_broker1:port1>[,kafka_broker2:port2] \
           -q <query>
```

You may run `docker run -it --rm python run.py --help` for more information.
