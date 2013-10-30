var Common={
	init:function(){
		this.setInput();
	},
	setInput:function(){
		$.sc2.placeholder({trigger:".popInput"});
	},
	getTimes:function(){
		return new Date().getTime();
	},
	setTooltips:function(obj,html){
	    obj.tooltips({
	      html: true,
	      placement: "bottom",
	      title: html,
	      delay: {
	        show: 100,
	        hide: 1000
	      }
	    });
	    		
	},
	getSelectTime:function(from,main){
		var startTime=from;
		$(".sqlTime",main).html("");
		var name=main.attr("name");

		Common.timer={};Common.timer[name]=null;
		Common.timer[name]=setInterval(function(){
			var currentTime=Common.getTimes(),
				ms=currentTime-startTime;

			var hLeave=ms%(24*3600*1000),
				remainH=Math.floor(hLeave/(3600*1000));

			var mLeave=hLeave%(3600*1000),
				remainM=Math.floor(mLeave/(60*1000));

			var sLeave=mLeave%(60*1000),
				remainS=Math.round(sLeave/1000);

				remainH<=9?remainH="0"+remainH:remainH=remainH;
				remainM<=9?remainM="0"+remainM:remainM=remainM;
				remainS<=9?remainS="0"+remainS:remainS=remainS;

				$(".sqlTime",main).html(remainH+":"+remainM+":"+remainS)							
		},1000);

	},
	formatTime:function(time){

		var date=new Date(time),
			y=date.getFullYear(),
			m=date.getMonth(),
			d=date.getDate(),
			h=date.getHours(),
			mi=date.getMinutes(),
			s=date.getSeconds();

				m<12?m=m+1:m=1;
				h<=9?h="0"+h:h=h;
				mi<=9?mi="0"+mi:mi=mi;
				s<=9?s="0"+s:s=s;			

			return y+"-"+m+"-"+d+" "+h+":"+mi+":"+s;
	},
	isLogin:function(){
        $.get(
        "/sql/", {
          "timestamp": Common.getTimes()
        },
        function(data,status) {

        },
        "json");
	},
	login:function(){
        $.post(
        "/sql/", {
          "email":"a@b.c",
          "password":"123",
          "timestamp": Common.getTimes()
        },
        function(data,status) {

        },
        "json");
	},
	slideClick:function(){
		var t=$(".sqlLogLink a");
		t.live("click",function(){
			var text=$(this).text();
			if(text=="收起日志"){
				$(this).find("strong").text("展开日志");
			}else{
				$(this).find("strong").text("收起日志");
			}
			$(this).toggleClass("active");
			$(this).parent().next().stop().slideToggle();
			return false;
		})
	},
	toBai:function(num,total){
		return Math.round(num / total * 10000) / 100.00 + "%";
	},
  setPop:function(poptitle,html){
    new Boxy(html, {title: poptitle,closeText:"",modal:true,unloadOnHide:true});
  },
  setGrid:function(title,value,poptitle){//列名，值，弹窗标题，是否要开启选项

	var params={
	  "id":"收集器ID",
	  "name":"数据收集器名称",
	  "recent-sync":"最后一次同步时间",
	  "synced-data":"数据传输总量",
	  "url":"收集器路径",
	  "status":"状态",
	  "操作":"操作"
	}    
    var values=value,
        column=title;
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
        "aaData": values,
        "aoColumns":allColumn,
        sScrollX:"100%",
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



  }    	
}