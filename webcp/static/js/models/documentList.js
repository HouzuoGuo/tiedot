App.DocumentList = Backbone.Collection.extend({
	
	url: function() {
		return '/query?col=' + this.id + '&q="all"';
	},

	queryUrl: function(query) {
		return '/query?col=' + this.id + '&q=' + query;
	},
	
	fetch: function(query) {
		var self = this;
		
		Backbone.ajax({
			url: query ? this.queryUrl(query) : this.url()
		})
		.done(function(res) {
			var documents = [];
			
			for (var id in res) {
				var document = res[id];
				document.id = id;
				
				documents.push(new App.Document(document));
			}
			
			self.reset(documents);
		})
		.fail(function(jqXHR, textStatus) {
			tiedotApp.notify('danger', 'Error running query: ' + jqXHR.responseText, 8000);
		});
	}
});