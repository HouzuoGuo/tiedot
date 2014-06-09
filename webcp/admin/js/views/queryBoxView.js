App.QueryBoxView = Backbone.View.extend({
	
	tagName: 'div',
	className: 'query-box',
	
	events: {
		'click .cancel': 'closeQueryBox',
		'click .query': 'onQueryClick'
	},
	
	initialize: function() {
		this.createQueryEditor();
		window.dispatcher.on('queryBox:close', this.closeQueryBox);
		window.dispatcher.on('queryBox:open', this.openQueryBox);
		
		this.delegateEvents();
	},
	
	setCol: function(col) {
		this.id = col;
	},

	openQueryBox: function(e) {
		$('#query-box').height(190);
	},
	
	closeQueryBox: function(e) {
		$('#query-box').height(0);
	},
	
	setQuery: function(query) {
		this.editor.getSession().setValue(query);
	},
	
	onQueryClick: function(e) {
		var query = this.editor.getSession().getValue();
		
		tiedotApp.router.navigate('query/' + this.id + '/' + query, { trigger: true });
	},
	
	createQueryEditor: function() {
		this.editor = ace.edit('query-box-editor');
		this.editor.getSession().setValue('{\n    \n}');
		
		this.editor.setOptions({
			minLines: 6,
			maxLines: 6
		});
	    this.editor.setTheme('ace/theme/github');
	    this.editor.getSession().setMode("ace/mode/json");
		this.editor.getSession().setTabSize(4);
		this.editor.getSession().setUseWrapMode(true);
		this.editor.setShowPrintMargin(false);
		this.editor.renderer.setShowGutter(false);
	}
});