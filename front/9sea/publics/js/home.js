
var Home = {
  init: function() {
    this.slide();
    this.login();
  },
  login:function(){
    var login_btn=$(".loginBtn");
    login_btn.click(function(){
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
              201:function(){location.href="/sql/"},
              401:function(){Boxy.alert("用户名或密码不正确")}
            }

        });
    })
  },
  slide:function(){
    var nav=$(".bannerNav a"),
        panels=$(".homeSlide > div");

    nav.hover(function(){
      var index=nav.index($(this));
      $(this).addClass("active").siblings().removeClass("active");
      panels.stop(true,true).eq(index).fadeIn().siblings().fadeOut();
    })    
  }

}
