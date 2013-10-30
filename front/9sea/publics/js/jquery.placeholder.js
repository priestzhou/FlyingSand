/*!
 * Base On jQuery JavaScript Library v1.4.2
 * http://jquery.com/
 * Libs:Jay placeholder
 * Copyright 2012, QinLiang
 * Dual licensed under the MIT or GPL Version 2 licenses.
 * http://jquery.org/license
 *
 * Date: 2012.10.22
 */

(function($) {
	var placeholder = {
		defaults: {
			trigger:".ui-placeholder"
		}
	};

	function Placeholder(defaults) {
		var self = this,
			element = $(defaults.trigger);

		$.extend(this, {
			isSupported:function(){
				var isInputSupported="placeholder" in document.createElement("input");
				var isTextareaSupported="placeholder" in document.createElement("textarea");
				return isInputSupported && isTextareaSupported;
			},
			setPlace:function(){
				if(this.isSupported()){
					return;	
				}else{
					element.each(function(){
						var arrtPlace=$(this).attr("placeholder");
						if(typeof arrtPlace=="undefined"){
							arrtPlace="";
						}					
						$(this).val(arrtPlace);
						$(this).focus(function(){
							var t=this;
							$(this).addClass("active");
							if(t.value==arrtPlace){
								t.value="";	
							}

						})
						$(this).blur(function(){
							var t=this;							
							if(t.value==""){
								$(this).removeClass("active");
								t.value=arrtPlace;
							}	
						})							
					})
					
				}
			},
			init: function() {
				this.setPlace();
			}
		});
		self.init();

	}

	$.sc2 = {
		placeholder: function(defaults) {
			defaults = $.extend({},
			placeholder.defaults, defaults);
			new Placeholder(defaults);
		}
	};
})(jQuery);