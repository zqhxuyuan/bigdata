app.directive('switch', function() {

	return {
		restrict : "A",
		link : function(scope, element, attrs) {
			element.attr("checked", scope.property.value == "true");
			$(element).bootstrapSwitch();

			$(element).on('switchChange.bootstrapSwitch', function(e, data) {
				scope.property.value = data.toString();
			});
		}
	};
});
