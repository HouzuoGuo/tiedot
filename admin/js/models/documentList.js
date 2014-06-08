App.DocumentList = Backbone.Collection.extend({
	
	url: function() {
		return '/query?col=' + this.id + '&q={"c":["all"]}';
	},
	
	fetch: function() {
		var self = this;
		
		Backbone.ajax({
			url: this.url()
		})
		.done(function(res) {
			var data = JSON.parse(res);
			var documents = [];
			
			for (var id in data) {
				var document = data[id];
				document.id = id;
				
				documents.push(new App.Document(document));
			}
			
			self.reset(documents);
		})
		.fail(function(jqXHR, textStatus) {
			alert('failed to load collections.');
		});
	}
});