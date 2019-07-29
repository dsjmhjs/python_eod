#!/usr/bin/env python
# _*_ coding:utf-8 _*_

from eod_aps.job import *


class DockerManager(object):
    def __init__(self, server_name):
        self.__server_name = server_name

    def run(self, container, port, path, docker_prot, docker_path, image):
        return 'docker run --name %s -p %s:%s -v %s:%s -t %s' % (container, port, docker_prot, path, docker_path, image)

    def start(self, container):
        return 'docker start %s' % container

    def stop(self, container):
        return 'docker stop %s' % container

    def restart(self, container):
        return 'docker restart %s' % container

    def status(self, container):
        return "docker ps | grep %s" % container

    def logs(self, container):
        return 'docker logs --tail=100 %s' % container

    def manager_docker(self, action, *container):
        server_model = server_constant.get_server_model(self.__server_name)
        if hasattr(DockerManager, action):
            action_cmd = getattr(self, action)(container)
            ret = server_model.run_cmd_str(action_cmd)
            return ret
        else:
            return False


if __name__ == '__main__':
    DockerManager('test').manager_docker('stop', 'ystest', 'asda', 'kkkkk')
