var Common={
	init:function(){
		this.setInput();		
	},
	uiInit:function(){
		this.selectDown();
	},
	setInput:function(){
		$.sc2.placeholder({trigger:".popInput"});

	},
	  login:function(){
	    var login_btn=$(".loginBtn");
	    login_btn.live("click",function(){
	      var html='<div class="loginPop">'+
	      '<input type="email" name="email" value="" placeholder="请输入您的用户名" class="boxy-content popInput">'+
	      '<input type="password" name="password" value="" placeholder="请输入您的密码" class="boxy-content popInput">'+
	      '<a class="loginPost btn_blue_long" href="#">登录</a>'+
	      '</div>';

	      Common.setPop("<span class='sqlIcon tipIcon tipPerIcon'></span>登录 WhaleMiner",html);
	      Common.setInput();
	      return false;
	    })

	    $(".loginPost").live("click",function(){
	        var email=$(".loginPop > input:eq(0)").val(),
	            password=$(".loginPop > input:eq(1)").val();
	        $.ajax({
	            url: '/sql/',
	            type: 'post',
	            data:{
	                      "email":email,
	                      "password":password
	            },
	            dataType: 'json',
	            error: function(){},
	            success: function(data){

	            },
	            statusCode:{
	              201:function(){location.href=location.href;},
	              401:function(){Boxy.alert("用户名或密码不正确")}
	            }

	        });
	    })
	  },
	delCookie:function(){
		var delLink=$(".loginOut");
		    var uid=$.cookie("user_id");
		    if(uid){
		      delLink.find("strong").html('退出登录');
		    }else{
		    	location.href="/sql/home.html";
		    	//delLink.find("strong").html('<a href="#" class="loginBtn">登录</a>');
		    }	
		
		delLink.click(function(){
			//$(this).find("strong").html('<a href="#" class="loginBtn">登录</a>');
			$.cookie("user_id",null,{path:"/sql/"});
			location.href="/sql/home.html";
		})
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
	getSelectTime:function(from,mainName){
		var startTime=from;
		
		Common.timer[mainName]=setInterval(function(){
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

				$("#sqlTab .body").find(".main[name="+mainName+"] .sqlTime").html(remainH+":"+remainM+":"+remainS);							
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
	  "id":"查询ID",
	  "name":"数据收集器名称",
	  "recent-sync":"最后一次同步时间",
	  "synced-data":"数据传输总量",
	  "url":"收集器路径",
	  "status":"状态",
	  "reason":"原因",
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
        "aaSorting": [[ 2, "desc" ]],
        bSortClasses:false,
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
  formatArr:function(arr){
 	  var column=[];	
      for (var j=0,l=arr.length;j<l;j++){
        if(typeof arr[j]!="undefined"){
          column.push(arr[j]);
        }
      }
      return column;
  },
  helpTabs:function(){
	var helpList=$(".helpList a"),helpCon=$(".helpCon .helpDetail");

	helpList.click(function(){
		var index=helpList.index($(this));
		$(this).parent().siblings().find("span").removeClass("openIcon").addClass("closeIcon");
		$(this).find("span").removeClass("closeIcon").addClass("openIcon");
		$(this).parent().siblings().removeClass("active");
		$(this).parent().addClass("active");	
		helpCon.eq(index).show().siblings().hide();

		return false;

	})  	
  },
  selectDown:function(){
  	var selectDown=$(".topMenu1"),select=$(".selectMenu"),timer;
  	selectDown.mouseenter(function(){
  		$(this).addClass("activeDown");
  		$(this).next().show();
  	})
  	selectDown.mouseleave(function(){
  		var t=$(this);
  		timer=setTimeout(function(){
  			t.removeClass("activeDown");
  			t.next().hide();
  		},800)
  	})  	

  	select.hover(function(){
  		clearTimeout(timer);
  	},function(){
  		$(this).hide();
  		$(this).prev().removeClass("activeDown");
  	})

  }    	
}
