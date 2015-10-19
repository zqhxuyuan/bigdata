app.controller('EditComponentCtrl', function($scope, $modalInstance, component, pipeline) {

	$scope.component = component;
	$scope.pipeline = pipeline;

	$scope.saveComponent = function() {
		$modalInstance.close($scope.component);
	};

	$scope.deleteComponent = function() {
		$modalInstance.close(null);
	};

});