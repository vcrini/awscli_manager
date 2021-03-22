#!/usr/bin/env python3

import argparse
import itertools
import json
import logging
import more_itertools
import re
import subprocess
import sys
import time
parser = argparse.ArgumentParser()
group = parser.add_mutually_exclusive_group()
group.add_argument('--active_services', action='store_true')
group.add_argument('--stop_services',
                   help="filename containing hash with services and desired count")
group.add_argument('--start_services',
                   help="filename containing hash with services and desired count")
parser.add_argument('--throttle', default=0.5)
parser.add_argument('--verbose', action='store_true')
parser.add_argument('--cluster', required=True)
args = parser.parse_args()
fmt = '%(message)s'
handler = logging.StreamHandler(sys.stdout)
if args.verbose:
    logging.basicConfig(format=fmt, level=logging.DEBUG, handlers=(handler,))
else:
    logging.basicConfig(format=fmt, level=logging.INFO, handlers=(handler,))

if args.active_services:
    cmd = ["aws", "ecs", "list-services", "--cluster", args.cluster]
    logging.debug(cmd)
    result = None
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT).stdout.decode('utf-8')
    except Exception as e:
        logging.error("aws-cli error  '{}' error: {}".format(' '.join(cmd), e))
        raise Exception
    logging.debug(result)
    list_services_json = json.loads(result)
    all_services = [re.search('([^\/]+)$', x).group(1)
                    for x in list_services_json['serviceArns']]
    grouped = more_itertools.chunked(all_services, 10)
    service = {}
    for g in grouped:
        cmd2 = ["aws", "ecs", "describe-services",
                "--cluster", args.cluster, "--services"]
        cmd = [x for x in itertools.chain(cmd2, g)]
        logging.debug(cmd)
        result = None
        try:
            result = subprocess.run(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT).stdout.decode('utf-8')
            logging.debug(g)
            logging.debug(result)
            list_services_desired_tasks_json = json.loads(result)
        except Exception as e:
            logging.error(
                "aws-cli error  '{}' error: {}".format(' '.join(cmd), e))
            raise Exception
        for x in list_services_desired_tasks_json['services']:
            service[x['serviceName']] = str(x['desiredCount'])
    logging.info(json.dumps(service))
if args.stop_services or args.start_services:
    f = args.stop_services if args.stop_services else args.start_services
    with open(f, "r") as fp:
        service = json.load(fp)
    logging.debug(service)
    for k in service.keys():
        if service[k] == "0":
            logging.info(
                "skipping {} since it's at desired task 0".format(k))
            continue
        if args.stop_services:
            desired_count = "0"
        elif args.start_services:
            desired_count = service[k]
        cmd = ["aws", "ecs", "update-service", "--cluster",
               args.cluster, "--service", k, "--desired-count", desired_count]
        try:
            result = subprocess.run(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT).stdout.decode('utf-8')
        except Exception as e:
            logging.error(
                "aws-cli error  '{}' error: {}".format(' '.join(cmd), e))
            raise Exception
        logging.info(
            "service {} set at {} desired_count ...".format(k, desired_count))
        time.sleep(int(args.throttle))
