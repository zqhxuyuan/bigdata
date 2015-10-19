app.factory('NotificationService', function() {

	return {
		notify : function(text, type) {
			$('#notifications').notify({
				message : {
					'text' : text
				},
				'closable' : false,
				'type' : type
			}).show();
		}
	};
});