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
			tiedotApp.notify('danger', 'Failed to load collections: ' + jqXHR.responseText, 8000);
		});
	},
	
	setDocumentCount: function(col, el) {
		Backbone.ajax({
			url: '/approxdoccount?col=' + col
		})
		.done(function(data) {
			$(el).html(numeral(data).format('0,0') + ' documents (approx.)');
		})
		.fail(function(jqXHR, textStatus) {
			return 0;
		});
	}
});