app.controller('CreatePipelineCtrl', function($scope, $modalInstance, info) {

	$scope.info = info;

	$scope.create = function() {
		$modalInstance.close($scope.info);
	};

	$scope.cancel = function() {
        $modalInstance.dismiss('cancel');
	};

});