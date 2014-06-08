App.CollectionList = Backbone.Collection.extend({
	
	url: function() {
		return '/all';
	},
	
	fetch: function() {
		var self = this;
		
		Backbone.ajax({
			url: this.url()
		})
		.done(function(data) {
			var cols = [];
			
			for (var c in data) {
				var col = data[c];
				
				cols.push(new App.Collection({ id: col }));
			}
			
			self.reset(cols);
		})
		.fail(function(jqXHR, textStatus) {
			alert('failed to load collections.');
		});
	}
});