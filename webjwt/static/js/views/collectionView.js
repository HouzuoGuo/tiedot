App.CollectionView = Backbone.View.extend({
	
	tagName: 'div',
	className: 'collection',
	template: _.template($('#collection-template').html()),
	
	events: {
	    'click .scrub': 'scrub',
		'click .rename': 'rename',
		'click .delete': 'delete',
		'click .search-link': 'onSearchLinkClick'
	},
	
	initialize: function() {
		this.listenTo(this.collection, 'reset', this.render);
		window.dispatcher.trigger('queryBox:close');
		
		this.collection.id = this.id;
		this.collection.fetch();
	},
	
	render: function() {
		var model = {
			name: this.id,
			docs: this.collection.toJSON(),
			total: numeral(this.collection.total).format('0,0'),
			page: parseInt(this.collection.page),
			totalPages: this.collection.totalPages,
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
	
	onSearchLinkClick: function(e) {
		window.dispatcher.trigger('queryBox:open');
	},

	scrub: function(e) {
		var self = this;

		e.preventDefault();

		var html = $('#collection-scrub-template').html();
		window.dispatcher.trigger('modal:open', html, function() {
			var that = this;

            $(that).find('.scrub').on('click', function(e) {
                window.dispatcher.trigger('modal:close');
                self.model.scrub();
            });
		});

		return false;
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