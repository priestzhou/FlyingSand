
var Query = {
  init: function() {
    //Common.isLogin();
    $.blockUI.defaults.message="加载中...";
    this.setTabs();
    this.setTreeData();
    this.setUi();
    this.setQuery();
    this.getQuery();
    //this.setDownQuery();
    Common.slideClick();
    this.doTablesOp();
    Common.delCookie();
    Common.login();
    Query.getResult={};
    Common.uiInit();

  },
  setUi: function() {
    this.setDownload();
  },
  setSelectTen:function(treeId, treeNode){
    var dom=$("#"+treeId+' a[title="table"]');
    var icon='<a class="selectTenL" href="#"><span class="sqlSIcon selectTen"></span></a>';
    dom.each(function(k,v){
      if ($(v).parent().find(".selectTen").length>0) return;
      $(v).after(icon);


    })

    dom.hover(function(){
      $(this).next().css("visibility","visible");
    },function(){
      //$(this).next().css("visibility","hidden");
    })


  },
  setTabsInt:function(data){
          var main=Query.getCurrentTab();
          var select_pro=$(".select_pro",main),select_ver=$(".select_ver",main),proArr=[],verArr=[];

          //var verObj={};
          for(var i=0,l=data.length;i<l;i++){
            proArr.push('<option>'+data[i].name+'</option>');
          }
            for(var j=0,le=data[0].children.length;j<le;j++){
              verArr.push('<option>'+data[0].children[j].name+'</option>');

            }

          $("#sqlTabCon .select_pro").html(proArr.join(''));
          $("#sqlTabCon .select_ver").html(verArr.join(''));
          select_pro.html(proArr.join(''));
          select_ver.html(verArr.join(''));

          select_pro.live("change",function(){
            var app=$(this).val(),verArr=[];
            for(var i=0,l=data.length;i<l;i++){
              if(data[i].name==app){
                for(var j=0,le=data[i].children.length;j<le;j++){
                  verArr.push('<option>'+data[i].children[j].name+'</option>');

                }
              }

            }
            $(this).next().html(verArr.join(''));

          })
  },
  setTreeData: function() {

/*    var uid=$.cookie("user_id");
    if(!uid){
      location.href="/sqldemo/";
      return;
    }*/

    var setting = {
      data: {
        key: {
          children: "children",
          title: "type",
          name:"name"
        }
      },
      view:{
        addDiyDom:Query.setSelectTen
      },
      callback: {
        onClick: function() {
          var t = Query.tree.getSelectedNodes(),
              type = t[0].type,
              name = t[0].name;
          var currentValue=Query.getCurrentValue();




          if (type == "table") {
            var ver=t[0].getParentNode(),
            app=ver.getParentNode(),
            verName=ver.name,
            appName=app.name,html="";

            if(appName==currentValue.app && verName==currentValue.ver){
              html="`"+name+"`";
            }else if(appName==currentValue.app && verName!=currentValue.ver){
              html="`"+verName+"`.`"+name+"`";
            }else{
              html="`"+appName+"`.`"+verName+"`.`"+name+"`";
            }

            var currentV=Query.getCurrentEditor().getValue();
            Query.getCurrentEditor().setValue(currentV+" "+html);
            return false;
          } else if (type == "namespace") {
            return false;
          } else {
            var pNode = t[0].getParentNode(),
                pNodeName=pNode.name,
                ver=pNode.getParentNode(),
                verName=ver.name,
                appName=ver.getParentNode().name,
                html = "";

            if(appName==currentValue.app && verName==currentValue.ver){
              html="`"+pNodeName+"`.`"+name+"`";
            }else if(appName==currentValue.app && verName!=currentValue.ver){
              html="`"+verName+"`.`"+pNodeName+"`.`"+name+"`";
            }else{
              html="`"+appName+"`.`"+verName+"`.`"+pNodeName+"`.`"+name+"`";
            }

            var currentV=Query.getCurrentEditor().getValue();
            Query.getCurrentEditor().setValue(currentV+" "+html);
            return false;
          }
        }
      }
    };
//tree data

    var t = $("#sqlTree");

    $(".selectTenL",t).live("click",function(){
            var tId=$(this).parent().attr("id"),
                t= Query.tree.getNodeByTId(tId);
                if(typeof t.samples=="undefined"){
                  Boxy.alert("暂无数据")
                  return false;
                }
            var children=t.children,
                values=t.samples,
                titles=[];

            for (var i=0,l=children.length;i<l;i++){
               titles.push(children[i].name);
            }

            Query.setPopGrid(titles,values,"<span class='sqlIcon tipIcon tipTenIcon'></span>前十条参考数据",true);
            return false;
    })
    $("#sqlTree").block();
    this.getMeta(t,setting);

  },
  getMeta:function(t,setting){
    $.get(
      "/sqldemo/meta", {

      },
      function(data,status) {
        $("#sqlTree").unblock();
        if(status=="success"){
          //var data=[{"children":[{"children":[{"type":"table","name":"acter","hive-name":"tn_9cbbdac653a98fc3a7a604c5ab7f88d11cf86534","children":[{"name":"id","type":"int"},{"name":"userid","type":"int"},{"name":"name","type":"string"},{"name":"class","type":"int"},{"name":"sex","type":"tinyint"},{"name":"hp","type":"int"},{"name":"mp","type":"int"},{"name":"rage","type":"int"},{"name":"str","type":"int"},{"name":"int","type":"int"},{"name":"agi","type":"int"},{"name":"spi","type":"int"},{"name":"sta","type":"int"},{"name":"level","type":"int"},{"name":"exp","type":"int"},{"name":"mapid","type":"int"},{"name":"instanceid","type":"int"},{"name":"x","type":"int"},{"name":"y","type":"int"},{"name":"money","type":"int"},{"name":"gift","type":"int"},{"name":"gold","type":"int"},{"name":"fastpay","type":"int"},{"name":"faction","type":"int"},{"name":"bagnum","type":"int"},{"name":"titlelist","type":"string"},{"name":"storagenum","type":"int"},{"name":"unionid","type":"int"},{"name":"ki","type":"int"},{"name":"kimax","type":"int"},{"name":"sitstarttime","type":"int"},{"name":"doingguidegroups","type":"string"},{"name":"finishedguidegroups","type":"string"},{"name":"flowercount","type":"int"},{"name":"eggcount","type":"int"},{"name":"autotaskflag","type":"int"},{"name":"skillpoint","type":"int"},{"name":"defaultmountid","type":"int"},{"name":"currentmountid","type":"int"},{"name":"attackmeridianid","type":"int"},{"name":"herosoulbagcount","type":"int"},{"name":"herosoulstoragecount","type":"int"},{"name":"heroactive","type":"string"},{"name":"herosoulfragment","type":"int"},{"name":"pvpprotect","type":"int"},{"name":"herosuper","type":"string"},{"name":"dantiancdfinishtime","type":"double"},{"name":"dantiancding","type":"int"},{"name":"activity","type":"int"},{"name":"issit","type":"tinyint"},{"name":"degreelevel","type":"int"},{"name":"degreeexp","type":"int"},{"name":"canreadbooks","type":"string"},{"name":"officialposition","type":"int"},{"name":"reputation","type":"int"},{"name":"herochallengelist","type":"string"},{"name":"herochallengemaxindex","type":"int"},{"name":"treasurehuntstate","type":"int"},{"name":"funcopenindex","type":"int"},{"name":"funcopenids","type":"string"},{"name":"specialtitles","type":"string"},{"name":"createtime","type":"int"},{"name":"charminglevel","type":"int"},{"name":"charmingexp","type":"int"},{"name":"score","type":"int"},{"name":"arenawincount","type":"int"},{"name":"arathiwincount","type":"int"},{"name":"escortbeautyid","type":"int"},{"name":"bestscore","type":"int"},{"name":"herochallengemaxtime","type":"double"},{"name":"astrallevel","type":"int"},{"name":"astralexp","type":"int"},{"name":"astralpoint","type":"int"},{"name":"nowastralid","type":"int"},{"name":"petheroidx","type":"int"},{"name":"petheroinneridx","type":"int"},{"name":"energy","type":"int"},{"name":"valentinetotal","type":"int"},{"name":"valentine","type":"int"},{"name":"fastpaybind","type":"int"},{"name":"fs_agent","type":"string"}]},{"type":"table","name":"acter_statistics_new","hive-name":"tn_c82ce1f1743f5d68e888f30101d4395427fc4457","children":[{"name":"acterid","type":"int"},{"name":"name","type":"string"},{"name":"value","type":"double"},{"name":"fs_agent","type":"string"}]},{"type":"table","name":"user","hive-name":"tn_ef7fd7e08e83918904ec37364288320f39fd7b70","children":[{"name":"id","type":"int"},{"name":"username","type":"string"},{"name":"passtype","type":"string"},{"name":"password","type":"string"},{"name":"adult","type":"tinyint"},{"name":"dt","type":"double"},{"name":"lastacterid","type":"int"},{"name":"banneduntil","type":"int"},{"name":"allowdebug","type":"int"},{"name":"qqgiftget","type":"int"},{"name":"isyellow","type":"int"},{"name":"isyearyellow","type":"int"},{"name":"yellowlevel","type":"int"},{"name":"source","type":"string"},{"name":"fs_agent","type":"string"}]}],"type":"namespace","name":"v1"}],"type":"namespace","name":"bigtable"},{"children":[{"children":[{"type":"table","name":"item","hive-name":"testapp","children":[{"name":"id","type":"int"},{"name":"name","type":"string"},{"name":"fs_agent","type":"string"}]}],"type":"namespace","name":"v1"},{"children":[{"type":"table","name":"item","hive-name":"testapp2","children":[{"name":"userid","type":"int"},{"name":"name","type":"string"},{"name":"fs_agent","type":"string"},{"name":"city","type":"string"}]}],"type":"namespace","name":"v2"}],"type":"namespace","name":"testapp"}];

          Query.tree = $.fn.zTree.init(t, setting, data);
          Query.setTreeHandle(Query.tree);
          Query.setTabsInt(data);
          Query.metaDataSave=data;
        }else{
          Query.getMeta(t,setting);
        }
      },
      "json");
  },
  setSelect:function(app,ver,data){
            var verArr=[],main=Query.getCurrentTab();
            $(".select_pro",main).val(app);
            for(var i=0,l=data.length;i<l;i++){
              if(data[i].name==app){
                for(var j=0,le=data[i].children.length;j<le;j++){
                  verArr.push('<option>'+data[i].children[j].name+'</option>');

                }
              }

            }
            $(".select_ver",main).html(verArr.join('')).val(ver);
  },
  setTreeHandle: function(obj) {
    var tree = $("#sqlTree"),
        treeOp = $(".treeOp a");

    treeOp.click(function() {
      var cl = $(this).attr("class");

      if (cl == "treeUp") {
        obj.expandAll(false);
      } else {
        obj.expandAll(true);
      }
    })
  },
  setCodeMirror:function(){
    Query.editor = CodeMirror.fromTextArea($(".sqlArea")[0], {
      mode: "text/x-mariadb",
      indentWithTabs: true,
      smartIndent: true,
      lineNumbers: true,
      matchBrackets : true,
      autofocus: true
    });
  },
  setTabs: function() {


    Query.tabs = $('#sqlTab');
    var dd = [{
      code: "tab0",
      title: "工作区",
      el: $("#sqlTabCon").html(),
      closeable: false
    }]
    Query.tabs.mac('tabs', {
      tabWidth: 80, //Use fix width
      items: dd,
      onCloseTab: function(me, c, a) {
        $(".sqlRTable div[rel="+c+"]").remove();
        //me.closeTab(c);
        //Boxy.confirm("确定删除？", function() {me.closeTab(c);return true;}, {title: "提示信息"});
        //$(this).parents(".item").remove();
        //$(".item[name="+c+"]").remove();
        return true;

      }
    }).selectFirst();

    this.setCodeMirror();

    var tabControl = $(".sqlTabControl"),
        addNew = $(".addNew", tabControl),
        delTabs=$(".delTabs", tabControl);

    addNew.click(function() {
      Query.setAddTabs(Query.tabs);
      return false;
    })

    delTabs.click(function(){
      //Boxy.confirm("确定删除？", function() {
        Query.tabs.select('tab0');
        Query.tabs.closeTabs();
      //}, {title: "提示信息"});

      return false;
    })


  },
  setAddTabs:function(tabs,value){
      var tab=$(".tt .item",tabs),
          i=tab.length;
      tabs.addTab({
        code: 'tab' + i,
        title: '工作区',
        closeable: true,
        el: $("#sqlTabCon").html()
      });
      tabs.select('tab' + i);
      //var editor = [];
      Query.editor[i] = CodeMirror.fromTextArea($(".sqlArea")[i], {
        mode: "text/x-mariadb",
        indentWithTabs: true,
        smartIndent: true,
        lineNumbers: true,
        matchBrackets: true,
        autofocus: true
      });

      var main=Query.getCurrentTab();
      $(".progress",main).css("display","none");
      $(".sqlTime",main).html("");
      $(".sqlRTable > div").hide();
      $(".sqlRTable div[rel=tab"+i+"]").show()
      //this.setCurrentTables(i);
      if(typeof value!=="undefined"){
        Query.editor[i].setOption("value", value);
      }
  },

  setGrid: function(title,value,id,url,mainName,total) {

var values=value,
    column=title;

    for (var i=0,l=values.length;i<l;i++){
      for (var j=0,jl=values[i].length;j<jl;j++){
        if(values[i][j]==null){
          values[i][j]="null";
        }
      }
    }
var allColumn=[];
    for (var i=0,l=column.length;i<l;i++){
      allColumn.push({"sTitle":column[i]});
    }
    var resultHtml='<div rel="'+mainName+'"></div>';
    if($(".sqlRTable div[rel="+mainName+"]").length>0){
      $(".sqlRTable div[rel="+mainName+"]").remove();
    }
    $(".sqlRTable").append(resultHtml);
    var currentResult=$(".sqlRTable div[rel="+mainName+"]");
    currentResult.html($("#tableResult").html());

    $(".sqlDownload",currentResult).attr("data-id",id).attr("data-url",url);
    Query.setDownQuery(currentResult);
    $(".sqlTableWrap",currentResult).html('<table cellpadding="0" cellspacing="0" border="0" class="display" id="sqlTable"></table>');
    $('#sqlTable',currentResult).dataTable({
      "bProcessing": true,
      "sPaginationType": "full_numbers",
      bLengthChange: false,
      bSort: false,
      "aaData": values,
      "aoColumns": allColumn,
      sScrollXInner:"110%",
      oLanguage: {
        "sInfo": "共 "+ total + " 条记录 _START_ 到 _END_ ",
        "sSearch": "搜索：",
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

  },

  setPopGrid:function(title,value,poptitle,option){//列名，值，弹窗标题，是否要开启选项

var params={
  "query":"查询内容",
  "name":"查询名称",
  "status":"查询状态",
  "app":"产品",
  "version":"版本",
  "操作":"操作",
  "db":"数据库",
  "submit_time":"查询提交时间",
  "duration": "耗时"
}
    var values=value,
        column=title;
    var allColumn=[];
    for (var i=0,l=column.length;i<l;i++){
      if(option){
        allColumn.push({"sTitle":column[i]});
      }else{
        allColumn.push({"sTitle":params[column[i]]});
      }

    }

    var popHtml='<div id="pop"><table cellpadding="0" cellspacing="0" border="0" id="popGrid"></table></div>';

    //var popHtml=$("#pop");
    new Boxy(popHtml, {title: poptitle,closeText:"",modal:true,unloadOnHide:true});

    if(option){
      $('#popGrid').dataTable({
        "bProcessing": true,
        bFilter:false,
        bInfo:false,
        bLengthChange:false,
        bPaginate:false,
        bSort:false,
        oLanguage:{
          "sProcessing":"加载中..."
        },
        "aaData": values,
        "aoColumns":allColumn
      });
    }else{

      $('#popGrid').dataTable({
        "bProcessing": true,
        "sPaginationType": "full_numbers",
        bLengthChange:false,
        bSort:false,
        "aaData": values,
        "aoColumns":allColumn,
        sScrollY:"300px",
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
/*      if(allColumn.length==4){

      }else{
        $("#pop th").css("width","14.2%");
        $("#popGrid td").css("width","14.2%");
      }*/

    }


  },
  setPop:function(poptitle,html){
    new Boxy(html, {title: poptitle,closeText:"",modal:true,unloadOnHide:true});
  },
  getCurrentTab:function(){
    return $("#sqlTab > .body > .main:visible");
  },
  getCurrentValue:function(){
    var currentTab=this.getCurrentTab(),
        app=$(".select_pro",currentTab).val(),
        ver=$(".select_ver",currentTab).val();

    return {
      app:app,
      ver:ver
    }
  },
  getCurrentEditor:function(){
    var main = Query.getCurrentTab(),
      index = main.attr("name").split("tab")[1];
      if(index==0){
        return Query.editor;
      }else{
        return Query.editor[index];
      }
  },
  getQueryOptions:function(){
    var main = Query.getCurrentTab(),
      index = main.attr("name").split("tab")[1],
      pro = $(".select_pro", main).val(),
      ver = $(".select_ver", main).val(),
      sql;
    if (index == 0) {
      sql = $.trim(Query.editor.getValue());
    } else {
      sql = $.trim(Query.editor[index].getValue());
    }

    return {
      pro:pro,
      ver:ver,
      sql:sql
    }
  },
  setQuery: function() {
    var submit = $(".sqlSelect:visible");
    //var submit = $("#sqlSelect");
    submit.live("click",function() {
      //var current=Query.getCurrentTab().attr("name");
      var submitT=$(this);
      if(submitT.data("n")==1){
        return;
      }

      var pro=Query.getQueryOptions().pro,
          ver=Query.getQueryOptions().ver,
          sql=Query.getQueryOptions().sql,
          main=Query.getCurrentTab(),
          mainName=main.attr("name");


      if(pro=="产品"){
          Boxy.alert("请选择产品");
          return false;
      }
      if(ver=="版本"){
          Boxy.alert("请选择版本");
          return false;
      }
      if(sql==""){
        Boxy.alert("请输入sql语句");
        return false;
      }

      submitT.block({ message: null });
      submitT.data("n",1)

                $.ajax({
                    url: "/sqldemo/queries/",
                    type: 'post',
                    data:{
                        "app":pro,
                        "version":ver,
                        "db":"",
                        "query":sql
                    },
                    dataType: 'json',
                    error: function(){submitT.unblock();},
                    success: function(data){
                        //$(".sqlSelect:visible").unblock();
                    },
                    statusCode:{
                      201:function(data){
                            var id=data.id;
                            var progressN=10;

                            //$("#sqlDownload").attr("data-id",id);

                              $.ajax({
                                  url: "/sqldemo/queries/"+id+"/",
                                  type: 'get',
                                  data:{
                                      "timestamp": Common.getTimes()
                                  },
                                  dataType: 'json',
                                  error: function(){submitT.unblock();},
                                  success: function(data){
                                      //$(".sqlSelect:visible").unblock();
                                  },
                                  statusCode:{
                                    200:function(data){

                                          $(".sqlLog",main).html("");
                                          if(data.status=="failed"){
                                            submitT.data("n",0);
                                            submitT.unblock();
                                            var error=data.error,
                                                log=data.log;
                                                $(".progress",main).css("display","none");
                                                $(".sqlTime",main).html("");
                                                $(".sqlLog",main).html(error+"<br/>"+log);

                                          }else if(data.status=="running"){

                                            //var now=Common.getTimes();
                                            Common.getSelectTime(Common.getTimes(),mainName);

                                            Query.getResult[mainName]=setInterval(function(){
                                                $.get(
                                                "/sqldemo/queries/"+id+"/", {
                                                  "timestamp": Common.getTimes()
                                                },
                                                function(data,status) {
                                                  if(status=="success"){

                                                    var progress=data.progress,
                                                        log=data.log;

                                                    var progressV=Common.toBai(progressN++,100);

                                                    $(".progress",main).css("display","inline-block");
                                                    if(progressN>=95){
                                                      $(".progress .bar",main).css("width","95%").text("95%");
                                                    }else{
                                                      $(".progress .bar",main).css("width",progressV).text(progressV);
                                                    }

                                                    $(".sqlLog",main).html($(".sqlLog",main).html()+"<br/>"+log);
                                                    if(data.status=="succeeded"){
                                                      submitT.data("n",0);
                                                      submitT.unblock();


                                                      var url ="/sql"+ data.url;
                                                      $(".progress .bar",main).css("width","100%").text("100%");
                                                      setTimeout(function(){$(".progress",main).css("display","none");},800);

                                                      clearInterval(Query.getResult[mainName]);
                                                      clearInterval(Common.timer[mainName]);
                                                      $(".sqlTime",main).html("本次查询执行时间："+$(".sqlTime",main).html());
                                                      Query.setGrid(data.result.titles,data.result.values,id,url,mainName,data.count);
                                                    }else if(data.status=="failed"){
                                                      submitT.data("n",0);
                                                      submitT.unblock();
                                                      var error=data.error,
                                                          log=data.log;

                                                          clearInterval(Query.getResult[mainName]);
                                                          clearInterval(Common.timer[mainName]);
                                                          $(".progress",main).css("display","none");
                                                          $(".sqlTime",main).html("");
                                                          $(".sqlLog",main).html(error+"<br/>"+log);

                                                    }
                                                  }else{
                                                      submitT.data("n",0);
                                                      submitT.unblock();
                                                      Boxy.alert("服务器已停止或服务器没有响应");
                                                  }
                                                },
                                                "json");
                                            },1000)

                                          }
                                    },
                                    401:function(){Boxy.alert("用户Id不存在")}
                                  }

                              });
                      },
                      401:function(){Boxy.alert("用户Id不存在")},
                      409:function(data){
                        Boxy.alert("参数错误,查询不符合sql的语法:"+data.error)
                      }
                    }

                });



        return false;

      })//click

  },
  setLog:function(data){

  },
  getQuery:function(){

    var sqlSave = $(".sqlSave"),
        openCommon=$(".openCommon"),
        openHistory=$(".openHistory");

    sqlSave.live("click",function() {

      var html='<input class="sqlSaveName popInput" placeholder="请输入名称" type="text" value=""/><p><a id="sqlSaveBtn" class="btn_blue_long" href="javascript:void(0);">保存</a></p>';
      Query.setPop("<span class='sqlIcon tipIcon tipCommonIcon'></span>保存为常用查询",html);
      Common.setInput();
    })


    $("#sqlSaveBtn").live("click",function(){

      var name=$.trim($(".sqlSaveName").val()),
          pro=Query.getQueryOptions().pro,
          ver=Query.getQueryOptions().ver,
          sql=Query.getQueryOptions().sql;

        if(name==""){
          Boxy.alert("请输入名称");
          return;
        }
        if(pro=="产品"){
            Boxy.alert("请选择产品");
            return false;
        }
        if(ver=="版本"){
            Boxy.alert("请选择版本");
            return false;
        }
        if(sql==""){
          Boxy.alert("请输入sql语句",function(){Query.delBoxy();});
          return false;
        }
        $.ajax({
            url: '/sqldemo/saved/',
            type: 'post',
            data:{
                      "name":name,
                      "app":pro,
                      "version":ver,
                      "db":"",
                      "query":sql
            },
            dataType: 'json',
            error: function(){},
            success: function(data){

            },
            statusCode:{
              409:function(){Boxy.alert("已经有该名称了，请重新输入")},
              201:function(){Boxy.alert("保存成功",function(){Query.delBoxy();})},
              401:function(){Boxy.alert("用户Id不正确")}
            }

        });

      return false;
    })
    openCommon.live("click",function(){
        $.get(
        "/sqldemo/saved/", {
          "timestamp": Common.getTimes()
        },
        function(data,status) {
          if(!data.length){Boxy.alert("暂时没有常用查询");return;}
          var titles=[],v=[],data_id=[];

          for (var j=0,l=data.length;j<l;j++){
              titles[j]=[];
              v[j]=[];
              for (var i in data[j]){
                if(i=="db"){
                  delete data[j][i];
                }else if(i=="id"){
                  data_id.push(data[j][i]);
                  delete data[j][i];
                }else{
                  titles[j].push(i);
                  v[j].push(data[j][i]);
                }

              }
              var del=Query.getTablesOp("del",data_id[j]),
                  edit=Query.getTablesOp("edit",data[j].query,data[j].app,data[j].version);
              v[j].push(del+" "+edit);
          }

          titles[0].push("操作");
          Query.setPopGrid(titles[0],v,"<span class='sqlIcon tipIcon tipCommonSIcon'></span>常用查询");
        },
        "json");
      return false;
    })

    openHistory.live("click",function(){
        $.get(
        "/sqldemo/queries/", {
          "timestamp": Common.getTimes()
        },
        function(data,status) {
          if(!data.length){Boxy.alert("暂时没有历史查询");return;}
          var titles = ["query", "status", "submit_time", "duration", "操作"];
          var v=[];

          for (var j=0,l=data.length;j<l;j++){
              v[j]=[];

              v[j].push(data[j].query);

              if (data[j].status == "succeeded") {
                v[j].push("成功");
              } else if (data[j].status == "failed") {
                v[j].push("失败");
              } else {
                v[j].push("运行中");
              }

              v[j].push(new Date(data[j].submit_time).toLocaleString());

              v[j].push(Math.round(data[j].duration / 1000) + " 秒");

              var edit=Query.getTablesOp("edit",data[j].query),
                  download=Query.getTablesOp("download",data[j].url);
              v[j].push(edit+" "+download);
          }
          Query.setPopGrid(titles,v,"<span class='sqlIcon tipIcon tipHistoryIcon'></span>历史查询");
        },
        "json");
        return false;
    })


  },
  getHistory:function(){
        $.get(
        "/sqldemo/queries/", {
          "timestamp": Common.getTimes()
        },
        function(data,status) {
          if(!data.length){return;}
          var titles = ["query", "status", "submit_time", "duration", "操作"];
          var v=[];

          for (var j=0,l=data.length;j<l;j++){
              v[j]=[];

              v[j].push(data[j].query);

              if (data[j].status == "succeeded") {
                v[j].push("成功");
              } else if (data[j].status == "failed") {
                v[j].push("失败");
              } else {
                v[j].push("运行中");
              }

              v[j].push(new Date(data[j].submit_time).toLocaleString());

              v[j].push(Math.round(data[j].duration / 1000) + " 秒");

              var edit=Query.getTablesOp("edit",data[j].query),
                  download=Query.getTablesOp("download",data[j].url);
              v[j].push(edit+" "+download);
          }

var params={
  "id":"查询ID",
  "item":"item",
  "query":"查询内容",
  "name":"查询名称",
  "status":"查询状态",
  "app":"产品",
  "version":"版本",
  "操作":"操作",
  "db":"数据库",
  "submit_time":"查询提交时间",
  "duration": "耗时"
}
            var values=v,
                column=titles;
            var allColumn=[];
            for (var i=0,l=column.length;i<l;i++){
              allColumn.push({"sTitle":params[column[i]]});
            }

            var html='<table cellpadding="0" cellspacing="0" border="0" id="colGrid"></table>';
            $("#sqlTableWrap").html(html);


              $('#colGrid').dataTable({
                "bProcessing": true,
                "sPaginationType": "full_numbers",
                bLengthChange:false,
                bSort:true,
                bSortClasses:false,
                "aaSorting": [[ 2, "desc" ]],
                "aaData": values,
                "aoColumns":allColumn,
                oLanguage:{
                  "sInfo": "共 _TOTAL_ 条记录 _START_ 到 _END_ ",
                  "sSearch":"搜索：",
                  "sProcessing":"加载中...",
                  "oPaginate": {
                    "sFirst": "首页",
                    "sLast": "尾页",
                    "sNext": "下一页",
                    "sPrevious": "上一页"
                  }
                }
              });



        },
        "json");
  },
  delQuery:function(){

  },
  getTablesOp:function(type,value){
    var opName="",cls="",data_app="",data_ver="";
    if(type=="del"){
      opName="删除";
      cls="tablesDel";
    }else if(type=="edit"){
      opName="编辑";
      cls="tablesEdit";
    }else if(type=="download"){
      opName="下载";
      cls="tablesDownload";
    }
    if(typeof arguments[2] !="undefined"){
      var data_app=' data-app='+arguments[2],
          data_ver=' data-ver='+arguments[3];
    }
    if(typeof value !="undefined"){
      value=value+"";
      var v=value.replace(/['"]/g,"\'");
    }else{
      var v="";
    }


    //var html="<a class="+cls+" rel='"+value+"' href='javascript:void(0);'"+data_app+data_ver+">"+opName+"</a>";
    var html='<a class='+cls+' rel="'+v+'" href="javascript:void(0);"'+data_app+data_ver+'>'+opName+'</a>';
    return html;
  },
  doTablesOp:function(){
    $(".tablesDel").live("click",function(){
        var t=$(this),id=t.attr("rel");
        $.ajax({
            url: '/sqldemo/saved/'+id+"/",
            type: 'delete',
            data:{

            },
            dataType: 'json',
            error: function(){},
            success: function(data){

            },
            statusCode:{
              200:function(){Boxy.alert("删除成功");t.parent().parent().remove();},
              401:function(){alert("该用户不存在")},
              404:function(){alert("该id不存在")}
            }

        });
    })
    $(".tablesEdit").live("click",function(){
        var sql=$(this).attr("rel");
        Query.getCurrentEditor().setValue(sql);
        if($(this).attr("data-app")){
          var data=Query.metaDataSave;
          Query.setSelect($(this).attr("data-app"),$(this).attr("data-ver"),data);
        }
        Query.delBoxy();

    })
    $(".tablesDownload").live("click",function(){
        var url=$(this).attr("rel");
        if(url=="null"){
          Boxy.alert("无可下载的记录");
        }else{
          location.href="/sqldemo/"+url;
        }

    })
  },
  setDownQuery:function(current){
    $(".downResult").live("click",function(){

          var qid=$(".sqlDownload",current).attr("data-id");
          var csv_url=$(".sqlDownload",current).attr("data-url");
          if(csv_url=="null"){
            Boxy.alert("无可下载的记录")
          }else{
            location.href=csv_url;
          }
/*          $.ajax({
              url: '/sqldemo/queries/'+qid,
              //url: csv_url,
              type: 'get',
              data:{
                        "timestamp": Common.getTimes()
              },
              dataType: 'json',
              error: function(){},
              success: function(data){

              },
              statusCode:{
                404:function(){alert("暂时没有结果")},
                200:function(){location.href=csv_url;}
              }

          });*/

        return false;
    })

    $(".downResultLink").live("click",function(){
        var csv_url=$(".sqlDownload",current).attr("data-url");
        window.prompt ("请按CTRL+C复制到剪贴板", location.href + csv_url);
        return false;
      });

  },
  setDownload:function(){
    var sqlDown = $(".sqlDownload"),
        tipHtml='<a class="downResult" href="#">下载结果</a> | <a class="downResultLink" href="javascript:void(0);">获取下载链接</a>';
    //Common.setTooltips(sqlDown,tipHtml);
    sqlDown.live("mouseenter",function(){
      $(this).find("p").css("visibility","visible");

    })
    sqlDown.live("mouseleave",function(){
      $(this).find("p").css("visibility","hidden");

    })

  },
  delBoxy:function(){
    $(".boxy-wrapper,.boxy-modal-blackout").remove();
  }
}
