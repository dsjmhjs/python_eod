#!/usr/bin/python
# -*- coding: utf-8 -*-
import mmap
import json
import traceback
from datetime import datetime

from eod_aps.model.server_model import ServerModel
from sqlalchemy.ext.declarative import DeclarativeMeta


class ObjectMmap(mmap.mmap):
    def __init__(self, fileno=-1, length=1024, access=mmap.ACCESS_WRITE, tagname='share_mmap'):
        super(ObjectMmap, self).__init__(self, fileno, length, access=access, tagname=tagname)
        self.length = length
        self.access = access
        self.tagname = tagname

    def jsonwrite(self, obj):
        try:
            self.obj = obj
            self.seek(0)
            obj_str = json.dumps(obj, cls=new_alchemy_encoder(), check_circular=False)
            obj_len = len(obj_str)
            content = str(obj_len) + ":" + obj_str
            self.write(content)
            self.contentbegin = len(str(obj_len)) + 1
            self.contentend = self.tell()
            self.contentlength = self.contentend - self.contentbegin
            return True
        except Exception, e:
            error_msg = traceback.format_exc()
            print error_msg
            return False

    def jsonread_master(self):
        try:
            self.seek(self.contentbegin)
            content = self.read(self.contentlength)
            obj = json.loads(content)
            self.obj = obj
            return obj
        except Exception, e:
            if self.obj:
                return self.obj
            else:
                return None

    def jsonread_follower(self):
        try:
            self.seek(0)
            index = self.find(":")
            if index != -1:
                head = self.read(index + 1)
                contentlength = int(head[:-1])
                content = self.read(contentlength)
                obj = json.loads(content)
                self.obj = obj
                return obj
            else:
                return None
        except Exception, e:
            if self.obj:
                return self.obj
            else:
                return None

def new_alchemy_encoder():
    _visited_objs = []

    class AlchemyEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj.__class__, DeclarativeMeta):
                # don't re-visit self
                if obj in _visited_objs:
                    return None
                _visited_objs.append(obj)

                # an SQLAlchemy class
                fields = {}
                for field in [x for x in dir(obj) if not x.startswith('_') and x != 'metadata']:
                    data = obj.__getattribute__(field)
                    try:
                        if isinstance(data, datetime):
                            data = data.strftime('%Y-%m-%d %H:%M:%S')
                        json.dumps(data)  # this will fail on non-encodable values, like other classes
                        fields[field] = data
                    except TypeError:
                        fields[field] = None
                return fields

            return json.JSONEncoder.default(self, obj)
    return AlchemyEncoder

# def new_alchemy_encoder():
#     _visited_objs = []
#
#     class AlchemyEncoder(json.JSONEncoder):
#         def default(self, obj):
#             _visited_objs.append(obj)
#             fields = {}
#             for field in [x for x in dir(obj) if not x.startswith('_') and x != 'metadata']:
#                 data = obj.__getattribute__(field)
#                 try:
#                     if isinstance(data, datetime):
#                         data = data.strftime('%Y-%m-%d %H:%M:%S')
#                     json.dumps(data)  # this will fail on non-encodable values, like other classes
#                     fields[field] = data
#                 except TypeError:
#                     fields[field] = None
#             return fields
#     return AlchemyEncoder