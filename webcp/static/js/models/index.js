App.Index = Backbone.Model.extend({
	
	createUrl: function() {
		return '/index?col=' + this.get('col') + '&path=' + this.get('path');
	},
	
	removeUrl: function() {
		return '/unindex?col=' + this.get('col') + '&path=' + this.get('path');
	},
	
	save: function() {
		Backbone.ajax({
			url: this.createUrl()
		})
		.done(function(res) {
			tiedotApp.notify('success', 'Index created successfully!');
		})
		.fail(function(jqXHR, textStatus) {
			tiedotApp.notify('danger', 'Failed to create index: ' + jqXHR.responseText, 8000);
		});
	},

	destroy: function() {
		var self = this;
		
		Backbone.ajax({
			url: this.removeUrl()
		})
		.done(function(res) {
			tiedotApp.notify('success', 'Index deleted successfully!');
		})
		.fail(function(jqXHR, textStatus) {
			tiedotApp.notify('danger', 'Failed to delete index: ' + jqXHR.responseText, 8000);
		});
	}

});