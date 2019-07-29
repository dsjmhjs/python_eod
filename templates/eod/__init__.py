# -*- coding: utf-8 -*-
from flask import Blueprint

eod = Blueprint('eod', __name__)

from . import views
