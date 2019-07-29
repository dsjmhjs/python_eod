# coding: utf-8
from flask import render_template, request, flash, redirect, url_for, jsonify
from . import position


@position.route('/position_manager', methods=['GET', 'POST'])
def manage_accounts():
    server_list = ['guoxin', 'huabao']
    return render_template('position/position_manager.html', server_list=server_list)



