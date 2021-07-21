#!/usr/bin/env python

import argparse
import datetime
import json
import logging
import os
import time
from hashlib import sha1

from kafka import KafkaProducer
from mediacloud.api import MediaCloud

logging.basicConfig(format="%(asctime)s %(levelname)s: %(name)s: %(message)s",
                    level=logging.INFO)
log = logging.getLogger(os.path.basename(__file__))


class Arguments(dict):
    """ Dictionary of arguments with dot notation. """
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def main(**args):
    """ Query MediaCloud for articles and stream them to Kafka. """
    args = Arguments(args) or get_args()
    last_id = args.last_id
    prev_time = time.time()

    since_date = datetime.datetime.strptime(args.since_date or datetime.datetime.today().strftime("%Y-%m-%d"), "%Y-%m-%d")
    until_date = datetime.datetime.strptime(args.until_date, "%Y-%m-%d") if args.until_date else since_date

    captured = 0
    finished = False
    results = []
    urls = set()

    if args.kafka_brokers:
        producer = KafkaProducer(bootstrap_servers=args.kafka_brokers,
                                 api_version=(0, 10, 0),
                                 value_serializer=lambda v:
                                 json.dumps(v).encode("utf-8"))

    if args.output_json:
        j = open(args.output_json, "w")

    mediacloud = MediaCloud(args.mediacloud_key)

    while not finished and since_date <= until_date:
        solr_filter = mediacloud.publish_date_query(until_date, until_date+datetime.timedelta(days=1))
        log.info(f"Collecting {solr_filter}...")

        while not finished:
            previous_results = results

            try:
                results = mediacloud.storyList(solr_query=args.query,
                                               solr_filter=solr_filter,
                                               last_processed_stories_id=last_id,
                                               rows=args.rows)

                if not results\
                or previous_results == results:
                    finished = True
                    break

                for story in results:
                    last_id = story["processed_stories_id"]

                    if args.lang and (story["language"] != args.lang):
                        continue

                    if story["url"]:
                        urls_length = len(urls)
                        urls.add(story["url"])

                        if urls_length != len(urls):
                            if args.kafka_brokers:
                                producer.send(args.topic,
                                              process_entry(story),
                                              key=sha1(story["url"].encode("utf-8")).hexdigest().encode("utf-8"))
                            if args.output_json:
                                json.dump(story, j)
                                j.write("\n")
                            captured += 1
                        else:
                            log.debug("Skipping duplicate document: %s" % story["url"])
                    else:
                        log.error(f"Found invalid document: {story}")

                    dt = time.time() - prev_time
                    if dt > 10:
                        log.info(f"Captured {captured} article(s).")
                        prev_time = time.time()

                    if args.limit == captured:
                        finished = True
                        break

            except Exception as e:
                log.warning(f"{e}")
                break

        until_date = until_date - datetime.timedelta(days=1)

    log.info(f"Total of {captured} captured article(s).")


def get_args():
    """ Read and return commandline arguments or show help text if necessary. """
    parser = argparse.ArgumentParser(description="""
        Command line interface for writing MediaCloud stories to Kafka.
        """)

    parser.add_argument("-b", "--kafka_brokers",
                        help="List of direct Kafka brokers",
                        required=True)
    parser.add_argument("-t", "--topic",
                        default="ingest.news")

    parser.add_argument("-k", "--mediacloud-key",
                        default=os.environ.get("MEDIACLOUD_KEY"),
                        help="user application key (required)")

    parser.add_argument("-q", "--query",
                        required=True,
                        help="to search tweets (required)")
    parser.add_argument("-l", "--lang",
                        help="2-letter code identifier (e.g. 'en' for english)")

    parser.add_argument("--since-date",
                        help="start gathering articles from date (YYYY-MM-DD)")
    parser.add_argument("--until-date",
                        help="continue gathering articles until date (YYYY-MM-DD)")

    parser.add_argument("--last-id",
                        help="oldest processed story ID",
                        type=int,
                        default=0)
    parser.add_argument("--limit",
                        help="maximum number of stories to capture",
                        type=int,
                        default=0)
    parser.add_argument("--rows",
                        help="batch size to get from MediaCloud",
                        type=int,
                        default=500)

    parser.add_argument("--output-json",
                        help="write articles to JSON file",
                        default=False)

    args = parser.parse_args()
    return args


def process_entry(story):
    return {
        "url": story["url"]
    }


if __name__ == "__main__":
    main()
