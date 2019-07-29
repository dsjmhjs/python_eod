# -*- coding: utf-8 -*-
from flask import Blueprint

summary = Blueprint('summary', __name__)

from . import views
