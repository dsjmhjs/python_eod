# -*- coding: utf-8 -*-
from flask import Blueprint

critical_job = Blueprint('critical_job', __name__)

from . import views
