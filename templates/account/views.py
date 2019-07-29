# coding: utf-8
from flask import render_template, request, flash, redirect, url_for, jsonify
from flask_login import login_required
from . import account
from .forms import AddAccountForm, EdiAccountForm, EdiAccountTradeRestrictions, AddUserForm
from eod_aps.model.schema_portfolio import RealAccount, AccountTradeRestrictions
from eod_aps.model.server_constans import server_constant


@account.route('/manage-accounts', methods=['GET', 'POST'])
@login_required
def manage_accounts():
    form = AddAccountForm()
    form2 = EdiAccountForm()

    form.allow_targets.choices = allow_targets_list

    if form.validate_on_submit():
        account_name = form.account_name.data
        query = session_portfolio.query(RealAccount)
        account_db = query.filter(RealAccount.accountname == account_name).first()
        if account_db:
            flash(u'添加账户失败！该账户名称已经存在。', 'danger')
        else:
            add_account = RealAccount()
            add_account.accountname = account_name
            add_account.accounttype = form.account_type.data
            add_account.accountconfig = form.account_config.data
            add_account.fund_name = form.fund_name.data

            allow_targets = form.allow_targets.data
            allow_target_list = []
            for (id, name) in allow_targets_list:
                if id in allow_targets:
                    allow_target_list.append('any,%s' % name)

            add_account.allow_targets = ';'.join(allow_target_list)
            add_account.enable = 1
            session_portfolio.add(add_account)
            session_portfolio.commit()

            accounttraderestrictions = AccountTradeRestrictions()
            accounttraderestrictions.build_default_restrictions(add_account.accountid)
            session_portfolio.add(accounttraderestrictions)
            session_portfolio.commit()
            flash(u'添加账户成功！', 'success')
        return redirect(url_for('.manage_accounts'))
    if form.errors:
        flash(u'添加账户失败！请查看填写有无错误。', 'danger')
        return redirect(url_for('.manage_accounts'))

    query = session_portfolio.query(RealAccount)
    accounts = []
    for account in query.order_by(RealAccount.accountid.desc()):
        accounts.append(account)
    return render_template('account/manage_accounts.html', accounts=accounts, endpoint='.manage_accounts',
                           form=form, form2=form2)

server_model = server_constant.get_server_model('host')
session_portfolio = server_model.get_db_session('portfolio')
session_common = server_model.get_db_session('common')
allow_targets_list = [(1, 'Commonstock'), (2, 'Option'), (3, 'Future')]
domain_list = [(1, 'PROD'), (2, 'QA')]


@account.route('/manage-accounts/edit-account', methods=['POST'])
@login_required
def edit_Account():
    form2= EdiAccountForm()
    if form2.validate_on_submit():
        account_id = form2.account_id.data
        query = session_portfolio.query(RealAccount)
        account_db = query.filter(RealAccount.accountid == account_id).first()
        account_db.accountname = form2.account_name.data
        account_db.accountconfig = form2.account_config.data
        account_db.enable = form2.enable.data
        session_portfolio.merge(account_db)
        session_portfolio.commit()
        flash(u'修改账户成功！', 'success')
        return redirect(url_for('.manage_accounts'))
    if form2.errors:
        flash(u'修改账户失败！请查看填写有无错误。', 'danger')
        return redirect(url_for('.manage_accounts'))


@account.route('/manage-accounts/delete-account/<int:id>')
@login_required
def delete_account(id):
    query = session_portfolio.query(RealAccount)
    account_db = query.filter(RealAccount.accountid == id).first()
    if not account_db:
        flash(u'警告：未找到要删除的条目！', 'danger')
        return redirect(url_for('.manage_accounts'))
    else:
        session_portfolio.delete(account_db)
        query_restrictions = session_portfolio.query(AccountTradeRestrictions)
        for accounttraderestrictions in query_restrictions.filter(AccountTradeRestrictions.account_id == account_db.accountid):
            session_portfolio.delete(accounttraderestrictions)
        session_portfolio.commit()
        flash(u'删除成功！', 'success')
    return redirect(url_for('.manage_accounts'))


@account.route('/manage-accounts/get-account-info/<int:id>')
@login_required
def get_account_info(id):
    query = session_portfolio.query(RealAccount)
    account_db = query.filter(RealAccount.accountid == id).first()
    return jsonify({
            'accountname': account_db.accountname,
            'accountconfig': account_db.accountconfig,
            'enable': account_db.enable
     })


@account.route('/manage-restrictions', methods=['GET', 'POST'])
@login_required
def manage_restrictions():
    form = EdiAccountTradeRestrictions()
    query = session_portfolio.query(AccountTradeRestrictions)
    restrictions = []
    for account in query.filter(AccountTradeRestrictions.ticker == 'all'):
        restrictions.append(account)
    return render_template('account/manage_restrictions.html', restrictions=restrictions, endpoint='.manage_accounts',
                           form=form)


@account.route('/manage-accounts/get-restriction-info/<int:id>')
@login_required
def get_restriction_info(id):
    query = session_portfolio.query(AccountTradeRestrictions)
    restrictions_db = query.filter(AccountTradeRestrictions.account_id == id, AccountTradeRestrictions.ticker == 'all').first()
    return jsonify({
        'account_id': float(restrictions_db.account_id),
        'max_operation': float(restrictions_db.max_operation),
        'max_cancel': float(restrictions_db.max_cancel),
        'max_order_flow_speed': float(restrictions_db.max_order_flow_speed),
        'max_cancel_ratio_threshold': float(restrictions_db.max_cancel_ratio_threshold),
        'max_cancel_ratio': float(restrictions_db.max_cancel_ratio),
        'min_fill_ratio_threshold': float(restrictions_db.min_fill_ratio_threshold),
        'min_fill_ratio_alarm': float(restrictions_db.min_fill_ratio_alarm),
        'min_fill_ratio_block': float(restrictions_db.min_fill_ratio_block),
        'max_buy_quota': float(restrictions_db.max_buy_quota)
     })


@account.route('/manage-accounts/edit-restriction', methods=['POST'])
@login_required
def edit_Restriction():
    form= EdiAccountTradeRestrictions()
    if form.validate_on_submit():
        account_id = form.account_id.data
        query = session_portfolio.query(AccountTradeRestrictions)
        restrictions_db = query.filter(AccountTradeRestrictions.account_id == account_id, AccountTradeRestrictions.ticker == 'all').first()
        restrictions_db.max_operation = form.max_operation.data
        restrictions_db.max_cancel = form.max_cancel.data
        restrictions_db.max_order_flow_speed = form.max_order_flow_speed.data
        restrictions_db.max_cancel_ratio_threshold = form.max_cancel_ratio_threshold.data
        restrictions_db.max_cancel_ratio = form.max_cancel_ratio.data
        restrictions_db.min_fill_ratio_threshold = form.min_fill_ratio_threshold.data
        restrictions_db.min_fill_ratio_alarm = form.min_fill_ratio_alarm.data
        restrictions_db.min_fill_ratio_block = form.min_fill_ratio_block.data
        restrictions_db.max_buy_quota = form.max_buy_quota.data
        session_portfolio.merge(restrictions_db)
        session_portfolio.commit()
        flash(u'修改风控成功！', 'success')
        return redirect(url_for('.manage_restrictions'))
    if form.errors:
        flash(u'修改风控失败！请查看填写有无错误。', 'danger')
        return redirect(url_for('.manage_restrictions'))


@account.route('/manage_users', methods=['GET', 'POST'])
@login_required
def manage_users():
    form = AddUserForm()
    form.domain.choices = domain_list
    if form.validate_on_submit():
        login_name = form.login_name.data
        query_sql = "select id from common.user where user_name='%s'" % login_name
        user_db = session_common.execute(query_sql).first()
        if user_db:
            flash(u'添加用戶失败！该用戶名称已经存在。', 'danger')
        else:
            query_sql = "select max(id) from common.user"
            max_id_db = session_common.execute(query_sql).first()
            user_id = max_id_db[0] + 1

            password = form.password.data
            domain_id = int(form.domain.data)
            inser_sql = "insert into common.user(id, user_name, password) values ('%s', '%s', '%s')" % (user_id, login_name, password)
            session_common.execute(inser_sql)
            session_common.commit()

            if domain_id == 1:
                report_str = 'UI.AlgoWin;UI.RiskMonitor;UI.OM;UI.Admin;UI.DM;UI.Settings'
            elif domain_id == 2:
                report_str = 'UI.ETFAMM;UI.AlgoWin;UI.RiskMonitor;UI.OM;UI.DM'
            inser_sql = "insert into common.user_domain(user_id, domain_id, ip, report, id)\
                values('%s', '1', '%%', '%s', '%s')" % (user_id, report_str, user_id)
            session_common.execute(inser_sql)
            session_common.commit()
            flash(u'添加用戶成功！', 'success')
        return redirect(url_for('.manage_users'))
    if form.errors:
        flash(u'添加用戶失败！请查看填写有无错误。', 'danger')
        return redirect(url_for('.manage_users'))

    query_sql = "select id, user_name from common.user"
    users = []
    for user_db in session_common.execute(query_sql):
        users.append(user_db)
    return render_template('account/manage_users.html', users=users, endpoint='.manage_users',
                           form=form)


@account.route('/manage-users/delete-user/<int:id>')
@login_required
def delete_user(id):
    query_sql = "select id from common.user where id='%s'" % id
    user_db = session_common.execute(query_sql).first()
    if not user_db:
        flash(u'警告：未找到要删除的条目！', 'danger')
        return redirect(url_for('.manage_users'))
    else:
        del_sql = "delete from common.user where id='%s'" % id
        session_common.execute(del_sql)
        session_common.commit()
        flash(u'删除成功！', 'success')
    return redirect(url_for('.manage_users'))
