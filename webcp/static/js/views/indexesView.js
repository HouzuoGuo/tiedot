App.IndexesView = Backbone.View.extend({
	
	tagName: 'div',
	className: 'document',
	template: _.template($('#indexes-template').html()),
	
	events: {
		'click .delete': 'onDeleteClick',
		'click .new-index': 'onNewIndexClick'
	},
	
	initialize: function(options) {
		this.listenTo(this.collection, 'reset', this.render);
		this.listenTo(this.collection, 'remove', this.render);
		this.listenTo(this.collection, 'add', this.render);
		window.dispatcher.trigger('queryBox:close');
		
		this.collection.id = this.id;
		this.collection.fetch();
	},
	
	render: function() {
		var model = {
			name: this.id,
			indexes: this.collection.toJSON()
		};
		
		this.$el.html(this.template(model));
		
		$('#app').html('');
		$('#app').append(this.$el);
		
		this.delegateEvents();
		return this;
	},
	
	onNewIndexClick: function(e) {
		var self = this;

		e.preventDefault();
		
		var html = $('#index-new-template').html();
		window.dispatcher.trigger('modal:open', html, function() {
			var that = this;
			
			$(that).find('.create').on('click', function(evt) {
				var path = $(that).find('.path').val().trim();
				window.dispatcher.trigger('modal:close');
				
				if (!path) {
					return;
				}
				
				var paths = path.split(',').map(function(p) { return p.trim(); });
				
				var index = new App.Index({ id: path, col: self.id, path: paths.join() });
				index.save();
				
				self.collection.add(index);
			});
		});

		return false;
	},

	onDeleteClick: function(e) {
		var self = this;

		e.preventDefault();
		
		var html = $('#index-delete-template').html();
		window.dispatcher.trigger('modal:open', html, function() {
			var that = this;
			
			$(that).find('.delete').on('click', function(evt) {
				window.dispatcher.trigger('modal:close');
				
				var index = self.collection.get($(e.currentTarget).data('id'));
				index.destroy();
				
				self.collection.remove(index);
			});
		});

		return false;
	}
});