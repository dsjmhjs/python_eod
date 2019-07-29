#!/usr/bin/env python
# _*_ coding:utf-8 _*_


def to_sql_many(obj_list, model_class, full_schema_name):
    column_str = model_class.get_column_list()
    value_list = []
    for obj in obj_list:
        value_str = obj.get_value_list
        value_list.append(value_str)

    sql = "REPLACE INTO %s %s VALUES %s " % (full_schema_name, column_str, ','.join(value_list))
    return sql


def to_many_sql(model_class, obj_list, full_schema_name, num=2000):
    sql_list = []
    tmp_obj_list = []
    for future_db in obj_list:
        tmp_obj_list.append(future_db)
        if len(tmp_obj_list) >= int(num):
            sql = to_sql_many(tmp_obj_list, model_class, full_schema_name)
            sql_list.append(sql)
            tmp_obj_list = []

    if len(tmp_obj_list) > 0:
        sql = to_sql_many(tmp_obj_list, model_class, full_schema_name)
        sql_list.append(sql)
    return sql_list
