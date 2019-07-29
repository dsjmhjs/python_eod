# -*- coding: utf-8 -*-
from flask import Blueprint
tool = Blueprint('tool_list', __name__)
from . import view