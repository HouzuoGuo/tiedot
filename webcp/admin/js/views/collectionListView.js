App.CollectionListView = Backbone.View.extend({
	
	tagName: 'div',
	className: 'collection-list',
	template: _.template($('#collection-list-template').html()),
	
	events: {
		'click #new-collection': 'newCollection'
	},
	
	initialize: function() {
		var self = this;
		
		this.listenTo(this.collection, 'reset', this.render);
		window.dispatcher.trigger('queryBox:close');
		
		this.collection.fetch();
	},
	
	render: function() {
		this.$el.html(this.template({ cols: this.collection.toJSON() }));
		
		$('#app').html('');
		$('#app').append(this.$el);
		this.setDocumentCounts();
		
		this.delegateEvents();
		return this;
	},
	
	setDocumentCounts: function() {
		var self = this;
		
		this.$('.collection').each(function(index, el) {
			self.collection.setDocumentCount($(el).data('id'), $(el).find('span'));
		});
	},
	
	newCollection: function(e) {
		var self = this;

		e.preventDefault();
		
		var html = $('#collection-new-template').html();
		window.dispatcher.trigger('modal:open', html, function() {
			var that = this;
			
			$(that).find('.create').on('click', function(e) {
				var name = $(that).find('.name').val().trim();
				window.dispatcher.trigger('modal:close');
				
				if (!name) {
					return;
				}
				
				new App.Collection({ id: name }).save();
			});
		});

		return false;
	}
});