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
group.add_argument('--start_pipeline_execution',
                   help="filename containing hash with pipeline names")
parser.add_argument('--throttle', default=0.5)
parser.add_argument('--verbose', action='store_true')
parser.add_argument('--cluster')
args = parser.parse_args()
fmt = '%(message)s'
handler = logging.StreamHandler(sys.stdout)
if not args.start_pipeline_execution and args.cluster is None:
    parser.error("--cluster is needed.")
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
if args.start_pipeline_execution:
    f = args.start_pipeline_execution
    with open(f, "r") as fp:
        service = json.load(fp)
    logging.debug(service)
    cmd_x = ["aws", "sts", "get-caller-identity"]
    result_x = json.loads(subprocess.run(cmd_x, stdout=subprocess.PIPE,
                                         stderr=subprocess.STDOUT).stdout.decode('utf-8'))
    if result_x['Account'] != "796341525871":
        print("This is not test, quitting ...")
        exit(0)
    else:
        print("This is test {}, continuing".format(result_x['Account']))
    for k in service.keys():
        # aws codepipeline start-pipeline-execution --name MyFirstPipeline
        cmd = ["aws", "codepipeline", "start-pipeline-execution", "--name", k, ]
        try:
            result = subprocess.run(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT).stdout.decode('utf-8')
        except Exception as e:
            logging.error(
                "aws-cli error  '{}' error: {}".format(' '.join(cmd), e))
            raise Exception
        logging.info(
            "pipeline {} starting ...".format(k))
        time.sleep(int(args.throttle))
