# -*- coding: utf-8 -*-
from flask import Blueprint

fund = Blueprint('fund', __name__)

from . import views
