# -*- coding: utf-8 -*-
import platform


class OpSysType(object):
    def __getattr__(self, attr):
        if attr == 'osx':
            return 'osx'
        if attr == 'rhel':
            return 'redhat'
        if attr == 'ubu':
            return 'ubuntu'
        if attr == 'fbsd':
            return 'FreeBSD'
        if attr == 'sun':
            return 'SunOS'
        if attr == 'unknown_linux':
            return 'unknown_linux'
        if attr == 'unknown':
            return 'unknown'
        else:
            raise AttributeError, attr

    def linuxType(self):
        if platform.dist()[0] == self.rhel:
            return self.rhel
        elif platform.uname()[1] == self.ubu:
            return self.ubu
        else:
            return self.unknow_linx

    def queryOS(self):
        if platform.system()[0] == 'Darwin':
            return self.osx
        elif platform.uname()[1] == 'Linux':
            return self.linuxType()
        elif platform.system() == self.sun:
            return self.sun
        elif platform.system() == self.fbsd:
            return self.fbsd

def fingerprint():
    type = OpSysType()
    print type.queryOS()

def UsePlatform():
  sysstr = platform.system()
  if sysstr == "Windows":
    print "Call Windows tasks"
  elif sysstr == "Linux":
    print "Call Linux tasks"
  else:
    print "Other System tasks"


if __name__ == '__main__':
    # fingerprint()
    UsePlatform()