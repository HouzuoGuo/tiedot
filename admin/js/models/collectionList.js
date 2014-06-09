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
			alert('Error trying to load collections.');
		});
	},
	
	setDocumentCount: function(col, el) {
		Backbone.ajax({
			url: '/count?col=' + col + '&q={"c":["all"]}'
		})
		.done(function(data) {
			var label = data == 1 ? ' document' : ' documents';
			
			$(el).html(data + label);
		})
		.fail(function(jqXHR, textStatus) {
			return 0;
		});
	}
});