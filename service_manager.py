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


def launch(cmd):
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.decode('utf-8')
def print_error(cmd, result, ex):
    logging.error(
        "aws-cli error  '{}->{}' error: {}".format(' '.join(cmd), result, ex))
    raise Exception(ex)


def show_pipeline(args):
    def multiple_in(values, name):
        prefixes = re.split(",\s*", values)
        for x in prefixes:
            if x in name:
                return True
    cmd = ["aws", "codepipeline", "list-pipelines"]
    logging.debug(cmd)
    cmd2 = None
    statuses = []
    result = None
    try:
        result = launch(cmd)
    except Exception as e:
        print_error(cmd, result, e)
    logging.debug(result)
    list_pipelines_json = json.loads(result)
    for x in list_pipelines_json['pipelines']:
        if multiple_in(args.value, x['name']):
            print(x['name'])
            cmd2 = ["aws", "codepipeline", "list-action-executions", "--pipeline-name",
                    x['name'], "--query", "actionExecutionDetails[0].[stageName,lastUpdateTime,status]"]
            try:
                result = launch(cmd2)
                statuses.append(json.loads(result))

            except Exception as e:
                print_error(cmd2, result, e)
            print(statuses)
            time.sleep(int(args.throttle))


def active_services(args):
    cmd = ["aws", "ecs", "list-services", "--cluster", args.cluster]
    logging.debug(cmd)
    result = None
    try:
        result = launch(cmd)
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
            result = launch(cmd)
            logging.debug(g)
            logging.debug(result)
            list_services_desired_tasks_json = json.loads(result)
        except Exception as e:
            print_error(cmd, result, e)
        for x in list_services_desired_tasks_json['services']:
            service[x['serviceName']] = str(x['desiredCount'])
    logging.info(json.dumps(service))


def stop_or_start(args):
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
            result = launch(cmd)
        except Exception as e:
            print_error(cmd, result, e)
        logging.info(
            "service {} set at {} desired_count ...".format(k, desired_count))
        time.sleep(int(args.throttle))


def start_pipeline_execution(args):
    f = args.start_pipeline_execution
    with open(f, "r") as fp:
        service = json.load(fp)
    logging.debug(service)
    for k in service.keys():
        # aws codepipeline start-pipeline-execution --name MyFirstPipeline
        cmd = ["aws", "codepipeline", "start-pipeline-execution", "--name", k, ]
        try:
            result = launch(cmd)
        except Exception as e:
            print_error(cmd, result, e)
        logging.info(
            "pipeline {} starting ...".format(k))
        time.sleep(int(args.throttle))


p = argparse.ArgumentParser(add_help=False)
p.add_argument('--throttle', default=0.5)
p.add_argument('--verbose', action='store_true')
p2 = argparse.ArgumentParser(add_help=False, parents=[p])
p2.add_argument('--cluster', required=True)
p3 = argparse.ArgumentParser(add_help=False)
p3.add_argument('value')
parser = argparse.ArgumentParser()
parser.add_argument('--throttle', default=0.5)
subparsers = parser.add_subparsers(
    help="desired action to perform", dest='action')
pipeline_parser = subparsers.add_parser(
    'show-pipeline', help="show current pipeline status, filtering them using prefixes separated by , ", parents=[p3, p])
pipeline_parser.set_defaults(func=show_pipeline)
active_services_parser = subparsers.add_parser(
    'active-services', help="show current active_services", parents=[p2])
active_services_parser.set_defaults(func=active_services)
stop_services_parser = subparsers.add_parser(
    'stop-services', help="filename containing hash with services to stop and desired count that are ignored", parents=[p3, p2])
stop_services_parser.set_defaults(func=stop_or_start)
start_services_parser = subparsers.add_parser(
    'start-services', help="filename containing hash with services and desired count, if 0 relative service is not started", parents=[p3, p2])
start_services_parser.set_defaults(func=stop_or_start)
start_pipeline_execution_parser = subparsers.add_parser(
    'start-pipeline-execution', help="filename containing hash with pipeline names", parents=[p3, p2])
start_pipeline_execution_parser.set_defaults(func=start_pipeline_execution)

args = parser.parse_args()
handler = logging.StreamHandler(sys.stdout)
fmt = '%(message)s'
if args.verbose:
    logging.basicConfig(format=fmt, level=logging.DEBUG, handlers=(handler,))
else:
    logging.basicConfig(format=fmt, level=logging.INFO, handlers=(handler,))
args.func(args)
exit()
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
if args.start_pipeline_execution:
    f = args.start_pipeline_execution
    with open(f, "r") as fp:
        service = json.load(fp)
    logging.debug(service)
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
