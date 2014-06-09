App.IndexList = Backbone.Collection.extend({
	
	url: function() {
		return '/indexes?col=' + this.id;
	},
	
	fetch: function(query) {
		var self = this;
		
		Backbone.ajax({
			url: this.url()
		})
		.done(function(res) {
			var data = res;
			var indexes = [];
			
			for (var id in data) {
				var index = {
					id: data[id].join(),
					col: self.id,
					path: data[id].join()
				};
				
				indexes.push(new App.Index(index));
			}
			
			self.reset(indexes);
		})
		.fail(function(jqXHR, textStatus) {
			tiedotApp.notify('danger', 'Error loading indexes: ' + jqXHR.responseText, 8000);
		});
	}
});