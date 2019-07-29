# -*- coding: utf-8 -*-
from flask import Blueprint
display_module = Blueprint('display_module', __name__)
from . import view