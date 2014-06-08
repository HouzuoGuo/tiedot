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
		
		this.collection.fetch();
	},
	
	render: function() {
		this.$el.html(this.template({cols: this.collection.toJSON() }));
		
		$('#app').html('');
		$('#app').append(this.$el);
		
		this.delegateEvents();
		return this;
	},
	
	newCollection: function(e) {
		var self = this;

		e.preventDefault();
		
		var html = $('#collection-new-template').html();
		window.dispatcher.trigger('modal:open', html, function() {
			var that = this;
			
			$(that).find('.create').on('click', function(e) {
				var name = $(that).find('.name').val().trim();
				var numparts = $(that).find('.numparts').val().trim();
				window.dispatcher.trigger('modal:close');
				
				if (!name) {
					return;
				}
				
				new App.Collection({ id: name, numparts: numparts }).save();
			});
		});

		return false;
	}
});