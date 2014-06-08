App.Collection = Backbone.Model.extend({
	
	saveUrl: function() {
		return '/create?col=' + this.id + '&numparts=' + this.get('numparts');
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

			window.dispatcher.trigger('col:add', { id: self.id, numparts: self.get('numparts') });
		})
		.fail(function(jqXHR, textStatus) {
			tiedotApp.notify('danger', 'Failed to create collection.');
		});
	},

	rename: function(name) {
		var self = this;
		
		Backbone.ajax({
			url: this.renameUrl(name)
		})
		.done(function(res) {
			window.dispatcher.trigger('col:remove', self);
			self.set('id', name);
			
			tiedotApp.router.navigate('cols/' + self.id, { trigger: true });
			tiedotApp.notify('success', 'Collection renamed successfully!');
			
			window.dispatcher.trigger('col:add', self);
		})
		.fail(function(jqXHR, textStatus) {
			tiedotApp.notify('danger', 'Failed to rename collection.');
		});
	},

	destroy: function() {
		var self = this;
		
		Backbone.ajax({
			url: this.deleteUrl()
		})
		.done(function(res) {
			window.dispatcher.trigger('col:remove', self);
			tiedotApp.router.navigate('/', { trigger: true });
			tiedotApp.notify('warning', 'Collection deleted successfully!');
		})
		.fail(function(jqXHR, textStatus) {
			tiedotApp.notify('danger', 'Failed to delete collection.');
		});
	}

});