import os
import csv
import uuid
import argparse
import subprocess
import itertools
import glob
import requests
from subprocess import CalledProcessError
from consul import Consul
from jinja2 import Environment, FileSystemLoader

DEFAULT_IMAGE = '****'
DEFAULT_NCPU = 4
DEFAULT_MEMORY = 2048
COMMAND_HEADER = 'python main.py '
JOB_HEADER = 'docker_test_'


def run(options):
    news_table = options.news_table
    news_date = options.news_date
    tweet_table = options.tweet_table

    # create unique job_id for each iteration
    job_id = JOB_HEADER + news_table + '_' + news_date + '_' + tweet_table

    command_updated = '{0} --news-table {1} --news-date {2} --tweet-table {3}'.format(COMMAND_HEADER, news_table, news_date, tweet_table)
    print("COMMAND UPDATED: " + command_updated)
    #print("ARGS(options): " + str(options))
    jf = Jfile(job_id=job_id, docker_image=options.docker_image,
               ncpu=options.ncpu, memory=options.memory, command=command_updated)
    jf.create_file(options.json_file, options.template)

    # Run chronos tasks if specified in options
    if options.run:
        cr = Chronos(consul_host=options.consul_host, port=options.port, dc=options.dc)
        print "------------------------------------------------------------------------------"
        print "Sending Job: {}".format(job_id)
        print "From file: {}".format(options.json_file)
        cr.chronos_submit(options.json_file)
        print "------------------------------------------------------------------------------"


class Chronos(object):
    """
    class Chronos
    """
    def __init__(self, consul_host='*****', port=8500, dc='****'):
        """
        init
        @param consul_host: mesos host
        @param port:        mesos port
        @param dc:          dc where mesos exists
        @return: None
        """
        self._consul_host = consul_host
        self._port = port
        self._dc = dc
        self._chronos_info = Consul(host=self._consul_host, port=self._port, dc=self._dc)\
                             .catalog.service('chronos')[1][0]
    @property
    def chronos_info(self):
        """
        Get chronos info object
        @return: self._chronos_info
        """
        return self._chronos_info

    def get_chronos_tasks(self):
        # Create query string to scheduler/jobs endpoint
        query = 'http://' + self.chronos_info['Address'] + ':' + \
                str(self.chronos_info['ServicePort']) + '/scheduler/jobs'
        tasks = requests.get(query)
        # Convert to json
        tasks = tasks.json()
        return tasks

    def chronos_submit(self, json_file):
        command = """curl -X POST -H \"Content-Type: application/json\"""" + \
                   """ --data-binary @{json_file} {chronos_ip}:{chronos_port}/scheduler/iso8601"""\
                    .format(json_file=json_file,
                            chronos_ip=self.chronos_info['ServiceAddress'],
                            chronos_port=self.chronos_info['ServicePort'])
        #print(command)
        print "Result:"
        try:
            subprocess.check_call(command, shell=True)
            print "\tJob send Success!"
        except CalledProcessError as e:
            print "\tUnable to send request to Chronos.\n" + \
                  "\tThe command run was: {}\n".format(command) + \
                  "\tIf this looks correct then check if Mesos or Chronos is down."
        return command


class Jfile(object):
    """
    class Jfile
    Used to create a JSON file from a jinja2 template for use with Chronos
    """
    def __init__(self, job_id='test1', docker_image=None, ncpu='4', memory='2048', command=None):
        self._docker_image = docker_image if not docker_image else DEFAULT_IMAGE
        self._command = command
        self._context = {
            'job_id': job_id,
            'docker_image': docker_image,
            'force_pull': False,
            'ncpu': ncpu,
            'memory': memory,
            'command': self._command
        }
    @property
    def context(self):
        return self._context
        
    def create_file(self, write_file, file_path='./templates/config.j2'):
        path, filename = os.path.split(file_path)
        rendered = Environment(
            loader=FileSystemLoader(path or './')
        ).get_template(filename).render(self.context)
        with open(write_file, 'w') as f:
            f.write(rendered)


if __name__ == "__main__":
    print("MAIN")
    parser = argparse.ArgumentParser(description="Create and send jobs to Chronos" + \
                                     "( must include '-r' option to send to Chronos)")
    parser.add_argument('-r', dest='run', action='store_true', help="Run Chronos Job")
    parser.add_argument('-n', dest='repeat', default=1, type=int,
                        help="Number of times to run job. Requires '-r' option")
    parser.add_argument('--consul_host', dest='consul_host', type=str,
                        default='*****',
                        help="Chronos host. Default:****** ")
    parser.add_argument('--port', dest='port', default=8500, type=int,
                        help="Chronos port. Default: 8500")
    parser.add_argument('--dc', dest='dc', default='**',
                        help="Chronos DC. Default: **")
    parser.add_argument('--json_file', dest='json_file', default='sim.json',
                        help="JSON file path. Default: 'sim.json'")
    parser.add_argument('--docker_image', dest='docker_image', default=DEFAULT_IMAGE,
                        help="Docker Image. Default: {0}".format(DEFAULT_IMAGE))
    parser.add_argument('--ncpu', dest='ncpu', type=int, default=DEFAULT_NCPU,
                        help="CPG for job. Default: 4")
    parser.add_argument('--memory', dest='memory', type=int, default=DEFAULT_MEMORY,
                        help="RAM in MB . Default: 2048")
    parser.add_argument('--template', dest='template', type=str,
                        default='./templates/config.j2',
                        help="jinja2 template path. Default: ./templates/config.j2")
    parser.add_argument('--news-table', dest='news_table', type=str,
                        default='default-table',
                        help="news-table")
    parser.add_argument('--news-date', dest='news_date', type=str,
                        default='default-date',
                        help="news-date")
    parser.add_argument('--tweet-table', dest='tweet_table', type=str,
                        default='tweet-table',
                        help="tweet-table")

    args = parser.parse_args()
    print(args)

    run(args)
