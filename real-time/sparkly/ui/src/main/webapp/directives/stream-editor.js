app.directive('inputStreamEditor', function($compile) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/component/input-stream-editor.html',
		scope : {
		    pipeline : "=pipeline",
			component : "=component",
			stream : "=stream",
		},
		link : function(scope, element, attrs) {
		    scope.availableFeatures = scope.pipeline.retrieveAvailableFeatures(scope.component, scope.stream);
		}
	};
});

app.directive('outputStreamEditor', function($compile) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/component/output-stream-editor.html',
		scope : {
		    pipeline : "=pipeline",
			component : "=component",
			stream : "=stream"
		}
	};
});