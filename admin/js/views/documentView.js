App.DocumentView = Backbone.View.extend({
	
	tagName: 'div',
	className: 'document',
	template: _.template($('#document-template').html()),
	
	events: {
		'click .delete': 'onDeleteClick',
		'click .save': 'onSaveClick'
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
		var json = this.model.toJSON();
		delete json.id;

		this.$el.html(this.template({ col: this.col, id: this.id }));
		
		$('#app').html('');
		$('#app').append(this.$el);
		
		this.createEditor(JSON.stringify(json, null, 4));
		return this;
	},

	renderNew: function() {
		var json = this.model.toJSON();
		delete json.id;

		this.$el.html(this.template({ col: this.col, id: 'New Document' }));
		
		$('#app').html('');
		$('#app').append(this.$el);

		this.createEditor('');
		return this;
	},
	
	createEditor: function(value) {
		this.editor = ace.edit('json');
		this.editor.getSession().setValue(value);
		
		this.editor.setOptions({
			minLines: 15,
			maxLines: 35
		});
	    this.editor.setTheme('ace/theme/github');
	    this.editor.getSession().setMode("ace/mode/json");
		this.editor.getSession().setTabSize(4);
		this.editor.getSession().setUseWrapMode(true);
		this.editor.setShowPrintMargin(false);
		//this.editor.renderer.setShowGutter(false);
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
	
	onSaveClick: function(e) {
		e.preventDefault();
		
		try {	
			var json = JSON.parse(this.editor.getValue());
		} catch(err) {
			tiedotApp.notify('danger', 'Invalid JSON. Unable to save document.');
			return false;
		}
		
		this.model.save(json);		
		
		return false;
	}
});