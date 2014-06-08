App.CollectionView = Backbone.View.extend({
	
	tagName: 'div',
	className: 'collection',
	template: _.template($('#collection-template').html()),
	
	events: {
		'click .rename': 'rename',
		'click .delete': 'delete'
	},
	
	initialize: function() {
		this.listenTo(this.collection, 'reset', this.render);
		
		this.collection.id = this.id;
		this.collection.fetch();
	},
	
	render: function() {
		var model = {
			name: this.id,
			docs: this.collection.toJSON(),
			docKeys: []
		};
		
		if (model.docs.length > 0) {
			model.docKeys = _.first(_.keys(model.docs[0]), 5);
		}
		
		this.$el.html(this.template(model));
		
		$('#app').html('');
		$('#app').append(this.$el);
		
		this.delegateEvents();
		return this;
	},
	
	rename: function(e) {
		var self = this;

		e.preventDefault();
		
		var html = $('#collection-rename-template').html();
		window.dispatcher.trigger('modal:open', html, function() {
			var that = this;
			$(that).find('.name').val(self.id);
			
			$(that).find('.rename').on('click', function(e) {
				var name = $(that).find('.name').val().trim();
				window.dispatcher.trigger('modal:close');
				
				if (!name) {
					return;
				}
				
				self.model.rename(name);
			});
		});

		return false;
	},
	
	delete: function(e) {
		var self = this;

		e.preventDefault();
		
		var html = $('#collection-delete-template').html();
		window.dispatcher.trigger('modal:open', html, function() {
			var that = this;
			
			$(that).find('.delete').on('click', function(e) {
				window.dispatcher.trigger('modal:close');
				
				self.model.destroy();
			});
		});

		return false;
	}
});