app.directive('propertyInput', function($compile) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/component/property-editor.html',
		scope : {
			property : "=model"
		}
	};
});