# -*- coding: utf-8 -*-
from flask import Blueprint

position = Blueprint('position', __name__)

from . import views