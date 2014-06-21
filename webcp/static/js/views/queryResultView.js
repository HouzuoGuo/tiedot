App.QueryResultView = Backbone.View.extend({
	
	tagName: 'div',
	className: 'query',
	template: _.template($('#query-template').html()),
	
	initialize: function() {
		this.listenTo(this.collection, 'reset', this.render);
		window.dispatcher.trigger('queryBox:open');
		
		this.collection.id = this.id;
		this.collection.query(this.model.get('query'));
		tiedotApp.queryBox.setCol(this.id);
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
	}
});