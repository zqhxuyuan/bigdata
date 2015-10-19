app.directive('tags', function() {

	return {
		restrict : "A",
		scope : {
            values : "=values",
            options : "=options"
        },
		link : function(scope, element, attrs) {

            var conf = {};
            if(scope.options) {
                conf.freeInput = false;
                conf.typeahead = {source: scope.options};
            }
			$(element).tagsinput(conf);

		    for(var i = 0; i < scope.values.length; i++) {
		        $(element).tagsinput('add', scope.values[i]);
		    }

			$(element).on('change', function(e, data) {
                scope.values.clear();
                scope.values.pushAll($(element).tagsinput('items'));
            });
		}
	};
});
