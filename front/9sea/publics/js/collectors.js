var Collectors = {
  init: function() {
    this.getCollectors();
    this.addCollectors();
    this.delCollectors();
    this.editCollectors();
    Common.delCookie();
    Common.login();
    this.getHistory();
  },
  addCollectors:function(){
    var add=$("#addCollectors");

    add.click(function(){
      var html='<input class="colName popInput" placeholder="请输入收集器名称" type="text" value=""/><input class="colUrl popInput" placeholder="请输入收集器地址" type="text" value=""/><p><a id="colSaveBtn" class="btn_blue_long" href="javascript:void(0);">确定</a></p>';
      Common.setPop("<span class='sqlIcon tipIcon'></span>添加数据收集器",html);
      Common.setInput();
    })

    $("#colSaveBtn").live("click",function(){
        var name=$(".colName").val(),
            url=$(".colUrl").val();
        $.ajax({
              url: "/sql/collectors/",
              type: 'post',
              data:{
                        "name": name,
                        "url": url,
                        "timestamp": Common.getTimes()
              },
              dataType: 'json',
              error: function(){},
              success: function(data){

              },
              statusCode:{
                201:function(){Boxy.alert("添加成功",function(){Query.delBoxy();Collectors.getCollectors();})},
                401:function(){},
                409:function(){}
              }

          });
    })


  },
  getCollectors:function(){
        $.ajax({
              url: "/sql/collectors/",
              type: 'get',
              data:{
                        "timestamp": Common.getTimes()
              },
              dataType: 'json',
              error: function(){},
              success: function(data){

              },
              statusCode:{
                401:function(){alert("暂时没有结果")},
                200:function(data){
                  if(!data.length){return;}
                  var titles=["name", "url", "status", "recent-sync", "操作"];
                  var v=[],stop=[];
                  for (var j=0,l=data.length;j<l;j++){
                      v[j] = [];
                      v[j].push(data[j].name);
                      v[j].push(data[j].url);

                      if (data[j].status == "no-sync") {
                        v[j].push("待同步");
                      } else if (data[j].status == "running") {
                        v[j].push("运行中");
                      } else if (data[j].status == "stopped") {
                        v[j].push("已停止");
                        stop.push(data[j].name)
                      } else if (data[j].status == "abandoned") {
                        v[j].push("已废弃");
                      }

                      if (data[j]["recent-sync"] == undefined || data[j]["recent-sync"] == null) {
                        v[j].push('--');
                      } else {
                        v[j].push(new Date(data[j]["recent-sync"]).toLocaleString())
                      }

                      var del=Collectors.getTablesOp("del",data[j].id);
                      var edit=""
                      if(data[j].status=="no-sync"){
                        var edit=Collectors.getTablesOp("edit",data[j].id+"__"+data[j].name+"__"+data[j].url)
                      }
                      v[j].push(del+" "+edit);
                  }


                  if(stop.length){
                    var tipHtml='<span class="sqlIcon tipsIcon"></span>'+
                    '您的'+stop.slice(0,3).join(",")+'已停止工作，部分数据没有同步，请通知您的系统管理员帮助重新开启数据收集器。'+
                    '<a href="#">我知道了</a>';
                    $(".colTips").html(tipHtml);
                    $(".colTips a").live("click",function(){
                      $(this).parent().html("");
                    })
                  }
                  
                  titles.push();
                  Common.setGrid(titles,v,"<span class='sqlIcon tipIcon'></span>常用查询");
                }
              }

          });

  },
  delCollectors:function(){

    var del=$(".colTable .collectorDel");

    del.live("click",function(){
        var id=$(this).attr("rel");

      var html='<textarea class="colDelTa popTextarea" placeholder="删除理由" value=""/><p><a id="colDel" class="btn_blue_long" href="javascript:void(0);" rel="'+id+'">提交</a></p>';
      Query.setPop("<span class='sqlIcon tipIcon tipCommonIcon'></span>删除收集器",html);
      
    })

    $("#colDel").live("click",function(){
        var id=$(this).attr("rel"),textareaVal=$(".colDelTa").val();
        if(textareaVal==""){
          Boxy.alert("请输入理由");
          return false;
        }
        $.ajax({
              url: "/sql/collectors/"+id,
              type: 'delete',
              data:{
                        "timestamp": Common.getTimes()
              },
              dataType: 'json',
              error: function(){},
              success: function(data){

              },
              statusCode:{
                404:function(){},
                401:function(){},
                200:function(){
                  Query.delBoxy();
                  Collectors.getCollectors();
                }
              }

          });
    })

  },
  editCollectors:function(){
    var edit=$(".colTable .collectorEdit");

    edit.live("click",function(){
        var rel=$(this).attr("rel"),
            data=rel.split("__");

        var id=data[0],name=data[1],url=data[2];

      var html='<input type="hidden" id="colEditId" value="'+id+'"><input class="colName popInput" type="text" value="'+name+'"/><input class="colUrl popInput" type="text" value="'+url+'"/><p><a id="colEditBtn" class="btn_blue_long" href="javascript:void(0);">确定</a></p>';
      Common.setPop("<span class='sqlIcon tipIcon'></span>修改路径",html);
    });

     $("#colEditBtn").live("click",function(){
        $.ajax({
              url: "/sql/collectors/"+ $("#colEditId").val(),
              type: 'put',
              data:{
                        "name": $(".colName").val(),
                        "url": $(".colUrl").val()
              },
              dataType: 'json',
              error: function(){},
              success: function(data){

              },
              statusCode:{
                404:function(){
                  Boxy.alert("该收集器不存在");
                },
                401:function(){},
                403:function(){},
                409:function(){
                  Boxy.alert("重复的收集器名称或url");
                },
                200:function(data){
                  Query.delBoxy();
                  Collectors.getCollectors();
                },
                500:function(){
                  Boxy.alert("服务器错误");
                }
              }

          });

     });

  },
  getTablesOp:function(type,value){
    var opName="",cls="";
    if(type=="del"){
      opName="废弃";
      cls="collectorDel";
    }else if(type=="edit"){
      opName="修改";
      cls="collectorEdit";
    }
    var html='<a class='+cls+' rel="'+value+'" href="javascript:void(0);">'+opName+'</a>';
    return html;
  },
  getHistory:function(){
    var obj=$("#checkCollectors");
    obj.click(function(){
        $.ajax({
              url: "/sql/collectors/",
              type: 'get',
              data:{
                        "timestamp": Common.getTimes()
              },
              dataType: 'json',
              error: function(){},
              success: function(data){

              },
              statusCode:{
                401:function(){alert("暂时没有结果")},
                200:function(data){
                  if(!data.length){return;}
                  var titles=[],v=[],data_id=[];

                  for (var j=0,l=data.length;j<l;j++){
                      if(data[j].status=="abandoned"){
                            titles[j]=[];
                            v[j]=[];

                            for (var i in data[j]){
                                if(i=="id"){
                                  data_id.push(data[j][i]);
                                  delete data[j][i];
                                }else{
                                  titles[j].push(i);
                                  v[j].push(data[j][i]);
                                }

                            }

                      }
              
                  }

                  Collectors.setPopGrid(titles,v,"<span class='sqlIcon tipIcon'></span>收集器历史操作");
                }
              }

          });
    })
  },
  setPopGrid:function(title,value,poptitle,option){//列名，值，弹窗标题，是否要开启选项
  var params={
    "reason":"备注",
    "name":"数据收集器名称",
    "recent-sync":"删除时间",
    "url":"收集器地址",
    "status":"状态"
  }
      var values=Common.formatArr(value),column=Common.formatArr(title);

      column=column[0];

      var allColumn=[];

      for (var i=0,l=column.length;i<l;i++){
        allColumn.push({"sTitle":params[column[i]]});
      }

      var popHtml='<div id="pop"><table cellpadding="0" cellspacing="0" border="0" id="popGrid"></table></div>';

      //var popHtml=$("#pop");
      new Boxy(popHtml, {title: poptitle,closeText:"",modal:true,unloadOnHide:true});



        $('#popGrid').dataTable({
          "bProcessing": true,
          "sPaginationType": "full_numbers",
          bLengthChange:false,
          bSort:false,
          bSortClasses:false,            
          "aaData": values,
          "aoColumns":allColumn,
          oLanguage:{
            "sInfo": "共 _TOTAL_ 条记录 _START_ 到 _END_ ",
            "sSearch":"搜索：",
            "sProcessing":"加载中...",
            "sZeroRecords":"无记录",
            "oPaginate": {
              "sFirst": "首页",
              "sLast": "尾页",
              "sNext": "下一页",
              "sPrevious": "上一页"
            }
          }
        });
    }
}