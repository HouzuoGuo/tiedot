App.DocumentList = Backbone.Collection.extend({
	
	url: function() {
		return '/getpage?col=' + this.id + '&page=' + (this.page - 1) + '&total=' + this.totalPages;
	},

	queryUrl: function(query) {
		return '/query?col=' + this.id + '&q=' + query;
	},

	countUrl: function() {
		return '/approxdoccount?col=' + this.id;
	},

	initialize: function(models, options) {
		if (options !== undefined && options.page) {
			this.page = options.page;
		} else {
			this.page = 1;
		}
	},
	
	fetch: function() {
		var self = this;
		
		this.getDocumentCount(function (count) {
			if (count === -1) {
				return;
			}
			self.total = count;
			self.totalPages = Math.ceil(count / 40);
			if (self.totalPages === 0.0) {
			    self.totalPages = 1;
			}

			Backbone.ajax({
				url: self.url()
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
				tiedotApp.notify('danger', 'Error loading documents: ' + jqXHR.responseText, 8000);
			});
		});
	},
	
	query: function(query) {
		var self = this;
		
		Backbone.ajax({
			url: this.queryUrl(query)
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
	},

	getDocumentCount: function(callback) {
		var self = this;

		Backbone.ajax({
			url: this.countUrl()
		})
		.done(function(res) {
			callback(res);
		})
		.fail(function(jqXHR, textStatus) {
			tiedotApp.notify('danger', 'Error getting document count: ' + jqXHR.responseText, 8000);
			callback(-1);
		});
	}
});