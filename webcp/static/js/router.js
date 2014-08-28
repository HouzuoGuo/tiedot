App.Router = Backbone.Router.extend({

	routes: {
		'': 'index',
		'cols/:name(/:page)': 'collectionByName',
		'indexes/:name': 'collectionIndexes',
		'docs/new/:col': 'newDoc',
		'docs/:col/:id': 'docById',
		'query/:col/:q': 'docsByQuery'
	},
	
	index: function() {
		var collectionsList = new App.CollectionListView({ collection: new App.CollectionList() });
	},
		
	collectionByName: function(name, page) {
		var collection = new App.CollectionView({ id: name, model: new App.Collection({ id: name }), collection: new App.DocumentList([], { page: page }) });
		tiedotApp.queryBox.setCol(name);
	},
	
	collectionIndexes: function(name) {
		var indexes = new App.IndexesView({ id: name, model: new App.Collection({ col: name }), collection: new App.IndexList() });
	},

	newDoc: function(col) {
		var documentView = new App.DocumentView({ col: col, model: new App.Document() });
		tiedotApp.queryBox.setCol(col);
	},
	
	docById: function(col, id) {
		var documentView = new App.DocumentView({ id: id, col: col, model: new App.Document({ id: id }) });
		tiedotApp.queryBox.setCol(col);
	},
	
	docsByQuery: function(col, q) {
		var queryResultView = new App.QueryResultView({ id: col, model: new App.Collection({ id: col, query: q }), collection: new App.DocumentList() });
		tiedotApp.queryBox.setQuery(q);
	}

});