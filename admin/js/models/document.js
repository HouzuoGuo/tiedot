App.Document = Backbone.Model.extend({
	collectionName: '',
	
	url: function() {
		return '/get?col=' + this.collectionName + '&id=' + this.id;
	},
	
	saveUrl: function(json) {
		return '/update?col=' + this.collectionName + '&id=' + this.id + '&doc=' + json;
	},
	
	insertUrl: function(json) {
		return '/insert?col=' + this.collectionName + '&doc=' + json;
	},
	
	deleteUrl: function() {
		return '/delete?col=' + this.collectionName + '&id=' + this.id;
	},
	
	save: function(json) {
		var self = this;
		
		if (this.id) {
			Backbone.ajax({
				url: this.saveUrl(JSON.stringify(json))
			})
			.done(function(res) {
				tiedotApp.notify('success', 'Document updated successfully!');
			})
			.fail(function(jqXHR, textStatus) {
				tiedotApp.notify('danger', 'Failed to save document: ' + jqXHR.responseText, 8000);
			});
		} else {
			Backbone.ajax({
				url: this.insertUrl(JSON.stringify(json))
			})
			.done(function(res) {
				tiedotApp.router.navigate('docs/' + self.collectionName + '/' + res, { trigger: true });
				tiedotApp.notify('success', 'Document added successfully!');
			})
			.fail(function(jqXHR, textStatus) {
				tiedotApp.notify('danger', 'Failed to save document: ' + jqXHR.responseText, 8000);
			});
		}
	},

	destroy: function() {
		var self = this;
		
		Backbone.ajax({
			url: this.deleteUrl()
		})
		.done(function(res) {
			tiedotApp.router.navigate('cols/' + self.collectionName, { trigger: true });
			tiedotApp.notify('warning', 'Document deleted successfully!');
		})
		.fail(function(jqXHR, textStatus) {
			tiedotApp.notify('danger', 'Failed to delete document: ' + jqXHR.responseText, 8000);
		});
	}

});