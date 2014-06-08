App.DocumentView = Backbone.View.extend({
	
	tagName: 'div',
	className: 'document',
	template: _.template($('#document-template').html()),
	
	events: {
		'click .delete': 'onDeleteClick',
		'click #document-form-submit': 'onSaveSubmit'
	},
	
	initialize: function(options) {
		_.extend(this, _.pick(options, 'col'));
		
		this.listenTo(this.model, 'change', this.render);
		
		this.model.collectionName = this.col;
		
		if (this.model.id) {
			this.model.fetch();
		} else {
			this.renderNew();
		}
	},
	
	render: function() {
		var viewModel = this.model.toJSON();
		delete viewModel.id;
		
		viewModel.json = JSON.stringify(viewModel, null, 4);
		viewModel.col = this.col;
		viewModel.id = this.id;
		
		this.$el.html(this.template(viewModel));
		
		$('#app').html('');
		$('#app').append(this.$el);
		return this;
	},

	renderNew: function() {
		var viewModel = {
			id: 'New Document',
			col: this.col,
			json: ''
		};
		
		this.$el.html(this.template(viewModel));
		
		$('#app').html('');
		$('#app').append(this.$el);
		return this;
	},
	
	onDeleteClick: function(e) {
		var self = this;

		e.preventDefault();
		
		var html = $('#document-delete-template').html();
		window.dispatcher.trigger('modal:open', html, function() {
			var that = this;
			
			$(that).find('.delete').on('click', function(e) {
				window.dispatcher.trigger('modal:close');
				
				self.model.destroy();
			});
		});

		return false;
	},
	
	onSaveSubmit: function(e) {
		e.preventDefault();
		
		try {	
			var json = JSON.parse($('#json').val());
		} catch(err) {
			tiedotApp.notify('danger', 'Error parsing JSON.');
			return false;
		}
		
		this.model.save(json);		
		
		return false;
	}
});