App.Collection = Backbone.Model.extend({
	
	saveUrl: function() {
		return '/create?col=' + this.id;
	},

	scrubUrl: function() {
		return '/scrub?col=' + this.id;
	},

	renameUrl: function(name) {
		return '/rename?old=' + this.id + '&new=' + name;
	},
	
	deleteUrl: function() {
		return '/drop?col=' + this.id;
	},
	
	save: function() {
		var self = this;
		
		Backbone.ajax({
			url: this.saveUrl()
		})
		.done(function(res) {
			tiedotApp.router.navigate('cols/' + self.id, { trigger: true });
			tiedotApp.notify('success', 'Collection created successfully!');
		})
		.fail(function(jqXHR, textStatus) {
			tiedotApp.notify('danger', 'Failed to create collection: ' + jqXHR.responseText, 8000);
		});
	},

	scrub: function() {
		var self = this;

		Backbone.ajax({
			url: this.scrubUrl()
		})
		.done(function(res) {
			tiedotApp.router.navigate('cols/' + self.id, { trigger: true });
			tiedotApp.notify('success', 'Scrub operation completed successfully!');
		})
		.fail(function(jqXHR, textStatus) {
			tiedotApp.notify('danger', 'Failed to scrub collection: ' + jqXHR.responseText, 8000);
		});
		
		tiedotApp.notify('warning', 'Scrub is in progress - it may take several minutes to complete...');
	},

	rename: function(name) {
		var self = this;
		
		Backbone.ajax({
			url: this.renameUrl(name)
		})
		.done(function(res) {
			self.set('id', name);
			
			tiedotApp.router.navigate('cols/' + self.id, { trigger: true });
			tiedotApp.notify('success', 'Collection renamed successfully!');
		})
		.fail(function(jqXHR, textStatus) {
			tiedotApp.notify('danger', 'Failed to rename collection: ' + jqXHR.responseText, 8000);
		});
	},

	destroy: function() {
		var self = this;
		
		Backbone.ajax({
			url: this.deleteUrl()
		})
		.done(function(res) {
			tiedotApp.router.navigate('/', { trigger: true });
			tiedotApp.notify('warning', 'Collection deleted successfully!');
		})
		.fail(function(jqXHR, textStatus) {
			tiedotApp.notify('danger', 'Failed to delete collection: ' + jqXHR.responseText, 8000);
		});
	}

});