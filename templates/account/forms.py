# coding: utf-8
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, TextAreaField, IntegerField, SelectMultipleField, SelectField
from wtforms.validators import DataRequired, InputRequired, Length, Email


class LoginForm(FlaskForm):
    email = StringField(u'电子邮件', validators=[DataRequired(), Length(1, 64),
                                             Email()])
    password = PasswordField(u'密码', validators=[DataRequired()])


class AddUserForm(FlaskForm):
    login_name = StringField(u'Login Name', validators=[DataRequired(), Length(1, 64)])
    password = PasswordField(u'Password', validators=[DataRequired()])
    domain = SelectField(u'Domain', coerce=int, validators=[DataRequired()])


class AddAccountForm(FlaskForm):
    account_name = StringField(u'Account Name', validators=[DataRequired(), Length(1, 64)])
    account_type = StringField(u'Account Type', validators=[DataRequired(), Length(1, 64)])
    account_config = TextAreaField(u'Account Config')
    allow_targets = SelectMultipleField(u'Allow Targets', coerce=int, validators=[DataRequired()])
    fund_name = StringField(u'Fund Name', validators=[DataRequired(), Length(1, 64)])


class EdiAccountForm(FlaskForm):
    account_id = StringField(u'Account Id', validators=[DataRequired()])
    account_name = StringField(u'Account Name', validators=[DataRequired(), Length(1, 64)])
    account_config = TextAreaField(u'Account Config')
    enable = StringField(u'Enable', validators=[DataRequired()])


class EdiAccountTradeRestrictions(FlaskForm):
    account_id = StringField(u'Account Id', validators=[DataRequired()])
    max_operation = IntegerField(u'max_operation', validators=[InputRequired()])
    max_cancel = IntegerField(u'max_cancel', validators=[InputRequired()])
    max_order_flow_speed = IntegerField(u'max_order_flow_speed', validators=[InputRequired()])
    max_cancel_ratio_threshold = IntegerField(u'max_cancel_ratio_threshold', validators=[InputRequired()])
    max_cancel_ratio = IntegerField(u'max_cancel_ratio', validators=[InputRequired()])
    min_fill_ratio_threshold = IntegerField(u'min_fill_ratio_threshold', validators=[InputRequired()])
    min_fill_ratio_alarm = IntegerField(u'min_fill_ratio_alarm', validators=[InputRequired()])
    min_fill_ratio_block = IntegerField(u'min_fill_ratio_block', validators=[InputRequired()])
    max_buy_quota = IntegerField(u'max_buy_quota', validators=[InputRequired()])