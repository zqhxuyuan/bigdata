app.directive('clickToEdit', function($compile) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/widgets/click-to-edit.html',
		scope : {
			model : "=",
		},controller: function($scope) {
            $scope.editing = false;
			$scope.edit = function() {
			    $scope.editing = true;
			},
			$scope.validate = function() {
                $scope.editing = false;
            }
        }
	};
});