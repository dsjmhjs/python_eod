/**
 * Created by luolinhua on 17-5-31.
 */

function create_multi_select(id_name, content){
    var html_content = '<div class="form-group" style="float:left;margin-left:7px">' +
        '<div style="padding-left:5px">' + id_name + '</div><div>' +
        '<select class="selectpicker form-control option multi" ' +
        'multiple data-live-search="true" data-max-option="1" id="' + id_name + '">';
    for(var j = 0; j < content.length;j++){
         html_content += '<option value="' + content[j] + '">' + content[j] + '</option>';
    }
    html_content += '</select></div></div>';
    return html_content
}