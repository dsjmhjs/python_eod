//JS For manage-articles when select articles or select comments
//JS For add articleType
$(document).ready(function() {
    $('.add-Account-btn').click(function() {
        $('#addAccountFormModel').modal('show');
    });
});

//JS For confirm to delete an articleType
function delAccount(url) {
    $('#delAccountCfmClick').click(function(){
        window.location.href = url;
    });
    $('#delAccountCfmModel').modal('show');
}

$(document).ready(function() {
    $('.add-User-btn').click(function() {
        $('#addUserFormModel').modal('show');
    });
});

//JS For confirm to delete an articleType
function delUser(url) {
    $('#delUserCfmClick').click(function(){
        window.location.href = url;
    });
    $('#delUserCfmModel').modal('show');
}

//JS For edit articleType to get its info
function get_account_info(url, id) {
    $.getJSON(url, function(data) {
        $('#account_id').val(id);
        $('#editAccountName').val(data.accountname);
        $('#editAccountConfig').val(data.accountconfig);
        $('#editEnable').val(data.enable);
        $('#ModalTitle').text('修改账户：' + data.accountname);
        $('#editAccountFormModel').modal('show');
    });
}

function get_restriction_info(url, id) {
    $.getJSON(url, function(data) {
        $('#editaccount_id').val(id);
        $('#editmax_operation').val(data.max_operation);
        $('#editmax_cancel').val(data.max_cancel);
        $('#editmax_order_flow_speed').val(data.max_order_flow_speed);
        $('#editmax_cancel_ratio_threshold').val(data.max_cancel_ratio_threshold);
        $('#editmax_cancel_ratio').val(data.max_cancel_ratio);
        $('#editmin_fill_ratio_threshold').val(data.min_fill_ratio_threshold);
        $('#editmin_fill_ratio_alarm').val(data.min_fill_ratio_alarm);
        $('#editmin_fill_ratio_block').val(data.min_fill_ratio_block);
        $('#editmax_buy_quota').val(data.max_buy_quota);
        $('#ModalTitle').text('修改风控：' + id);
        $('#editRestrictionFormModel').modal('show');
    });
}

