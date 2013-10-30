
var Collectors = {
  init: function() {
    this.getCollectors();
    this.addCollectors();
    this.delCollectors();
    this.editCollectors();
    Common.delCookie();
    Common.login();

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
                  var titles=["name", "url", "status", "recent-sync", "synced-data"];

                  for (var j=0,l=data.length;j<l;j++){
                      if(data[j].status=="no-sync"){
                        data[j]["recent-sync"]="--";
                        data[j]["synced-data"]="--";
                      }
                  }
                  var v=[];
                  for (var j=0,l=data.length;j<l;j++){
                      v[j] = [];
                      for (var i in titles) {
                        i = titles[i];
                        if (data[j][i] == "no-sync") {
                          v[j].push("未同步");
                        } else if (data[j][i] == "running") {
                          v[j].push("运行中");
                        } else if (data[j][i] == "stopped") {
                          v[j].push("已停止");
                        } else {
                          v[j].push(data[j][i]);
                        }
                      }

                      var del=Collectors.getTablesOp("del",data[j].id);
                      var edit=""
                      if(data[j].status=="no-sync"){
                        var edit=Collectors.getTablesOp("edit",data[j].id+"__"+data[j].name+"__"+data[j].url)
                      }
                      v[j].push(del+" "+edit);
                  }

                  titles.push("操作");
                  Common.setGrid(titles,v,"<span class='sqlIcon tipIcon'></span>常用查询");
                }
              }

          });
  },
  delCollectors:function(){

    var del=$(".colTable .collectorDel");

    del.live("click",function(){
        var id=$(this).attr("rel");
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

      var html='<input class="colName popInput" type="text" value="'+name+'"/><input class="colUrl popInput" type="text" value="'+url+'"/><p><a id="colEditBtn" class="btn_blue_long" href="javascript:void(0);">确定</a></p>';
      Common.setPop("<span class='sqlIcon tipIcon'></span>修改路径",html);

       $("#colEditBtn").one("click",function(){
          $.ajax({
                url: "/sql/collectors/"+id,
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

       })

    })


  },
  getTablesOp:function(type,value){
    var opName="",cls="";
    if(type=="del"){
      opName="删除";
      cls="collectorDel";
    }else if(type=="edit"){
      opName="修改路径";
      cls="collectorEdit";
    }
    var html='<a class='+cls+' rel="'+value+'" href="javascript:void(0);">'+opName+'</a>';
    return html;
  }
}
