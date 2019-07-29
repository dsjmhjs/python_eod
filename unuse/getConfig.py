import os
path = os.path.dirname(__file__)

def getConfig():
    cfg_dict = dict()
    fr = open(path + '/../../config.txt')
    for line in fr:
        l = line.strip()
        if (l=='') or (l[0]=='#'):
            continue
        if l=='<-mysql--':
            pass
        elif l=='--mysql->':
            pass
        else:
            [key,data] = l.split('=')
            cfg_dict[key]=data
    fr.close()
    return cfg_dict
