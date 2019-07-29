# -*- coding: utf-8 -*-
from flask import Blueprint

system = Blueprint('system', __name__)

from . import views
