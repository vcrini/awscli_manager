#!/usr/bin/env python3

import argparse
import copy
# import cProfile
# import pstats
import itertools
import json
import logging
import more_itertools
import operator
import re
import subprocess
import sys
import time


def launch(cmd):
    def is_error(result):
        if re.search("An error occurred", result):
            return (True, result)
        else:
            return (False, None)

    def raise_error(result):
        logging.debug(result)
        r = is_error(result)
        if r[0]:
            raise Exception(r[1])

    result = subprocess.run(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT).stdout.decode('utf-8')
    raise_error(result)
    return result


def print_error(cmd, result, ex):
    logging.error(
        "aws-cli error  '{}->{}' error: {}".format(' '.join(cmd), result, ex))
    raise Exception(ex)


def show_pipeline(args):
    def multiple_in(values, name):
        prefixes = re.split(r',\s*', values)
        for x in prefixes:
            if re.search(x, name):
                return True

    cmd = ["aws", "codepipeline", "list-pipelines"]
    cmd2 = None
    statuses = []
    result = None
    deploy = None
    try:
        result = launch(cmd)
    except Exception as e:
        print_error(cmd, result, e)
    list_pipelines_json = json.loads(result)
    keys = ['status', 'stageName', 'startTime', 'lastUpdateTime']
    keys_with_name = copy.copy(keys)
    keys_with_name.insert(0, 'name')

    for x in list_pipelines_json['pipelines']:
        if multiple_in(args.value, x['name']) and not re.search(r'deploy$', x['name']):
            cmd2 = ["aws", "codepipeline", "list-action-executions",
                    "--pipeline-name", x['name']]
            try:
                result = launch(cmd2)
                deploy = json.loads(result)
                filtered_deploy = {
                    k: deploy['actionExecutionDetails'][0][k] for k in keys}
                if not args.status or (args.status and filtered_deploy['status'] in args.status):
                    filtered_deploy.update({'name': x['name']})
                    statuses.append(filtered_deploy)
            except IndexError:
                logging.debug("Skipping because pipeline {}  contains no data: {}".format(
                    x['name'], deploy))
            except Exception as e:
                print_error(cmd2, result, e)
            time.sleep(float(args.throttle))
    p = [json.dumps({k: x[k] for k in keys_with_name}, indent=2) for x in sorted(
        statuses, key=operator.itemgetter('lastUpdateTime'), reverse=args.reverse)]
    print("[{}]".format(",".join(p)))


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
    all_services = [re.search(r'([^\/]+)$', x).group(1)
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
    f = args.value
    desired_count = None
    with open(f, "r") as fp:
        service = json.load(fp)
    logging.debug(service)
    for k in service.keys():
        result = None
        if service[k] == "0":
            logging.info(
                "skipping {} since it's at desired task 0".format(k))
            continue
        if args.action == 'stop-services':
            desired_count = "0"
        elif args.action == 'start-services':
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
    f = args.value
    result = None
    with open(f, "r") as fp:
        service = json.load(fp)
    logging.debug(service)
    for k in service.keys():
        # aws codepipeline start-pipeline-execution --name MyFirstPipeline
        cmd = ["aws", "codepipeline", "start-pipeline-execution", "--name", k, ]
        try:
            result = launch(cmd)
            logging.debug(result)
        except Exception as e:
            print_error(cmd, result, e)
        logging.info(
            "pipeline {} starting ...".format(k))
        time.sleep(int(args.throttle))


def disable_or_enable_stage(args):
    f = args.value
    result = None
    action = args.action
    with open(f, "r") as fp:
        service = json.load(fp)
    logging.debug(service)
    for k in service:
        # aws codepipeline start-pipeline-execution --name MyFirstPipeline
        cmd = ["aws", "codepipeline", "{}-stage-transition".format(action), "--pipeline-name", k, "--stage-name", "Source", "--transition-type", "Outbound"]
        if action == "disable":
            cmd.extend(["--reason", "updated by service_manager.py"])
        try:
            result = launch(cmd)
            logging.debug(result)
        except Exception as e:
            print_error(cmd, result, e)
        logging.info(
            "pipeline {} updating ...".format(k))
        time.sleep(int(args.throttle))

# import pdb; pdb.set_trace()
# p = argparse.ArgumentParser(add_help=False)
# p.add_argument('--throttle', default=0.5)
# p.add_argument('--verbose', action='store_true', default=False)


p2 = argparse.ArgumentParser(add_help=False)
p2.add_argument('--cluster', required=True)
p3 = argparse.ArgumentParser(add_help=False)
p3.add_argument('value')
parser = argparse.ArgumentParser()
parser.add_argument('-t', '--throttle', default=0.5)
parser.add_argument('-v', '--verbose', action='store_true',)
subparsers = parser.add_subparsers(
    help="desired action to perform", dest='action')
pipeline_parser = subparsers.add_parser(
    'show-pipeline', help="show current pipeline status, filtering them using prefixes separated by , ", parents=[p3])
pipeline_parser.add_argument(
    '--status', choices=['InProgress', 'Succeeded', 'Failed'])
pipeline_parser.add_argument(
    '--reverse', action="store_true", default=False, help="to invert sorting order")
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
stage_parser = subparsers.add_parser('pipeline', help="disable or enable stage transition, pass a file containing list of services like [\"service1\", \"service2\"] ", parents=[p3])
stage_parser.set_defaults(func=disable_or_enable_stage)
stage_parser.add_argument(
    '--action', choices=['enable', 'disable'])

args = parser.parse_args()
handler = logging.StreamHandler(sys.stdout)
fmt = '%(message)s'
if args.verbose:
    logging.basicConfig(format=fmt, level=logging.DEBUG, handlers=(handler,))
else:
    logging.basicConfig(format=fmt, level=logging.INFO, handlers=(handler,))
try:
    args.func(args)
except AttributeError:
    parser.print_help(sys.stderr)
# profile = cProfile.Profile()
# profile.runcall(show_pipeline,args)
# ps = pstats.Stats(profile)
# ps.print_stats()
# aws-vault exec  gucci-test-admin -- aws codepipeline disable-stage-transition --pipeline-name fdh-dondit --stage-name Source --transition-type Outbound --reason test
# aws-vault exec  gucci-test-admin -- aws codepipeline enable-stage-transition --pipeline-name fdh-dondit --stage-name Source --transition-type Outbound
