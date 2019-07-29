# -*- coding: utf-8 -*-
from flask import Blueprint

cta = Blueprint('cta', __name__)

from . import views
