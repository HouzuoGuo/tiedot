var tiedotApp;

App.AppView = Backbone.View.extend({
	el: $("#app"),
	router: {},
	
	initialize: function() {
		this.modal = App.Modal();
		this.modal.init();

		$('.load-doc').on('submit', this.onLoadDocFormSubmit);
		
		var collectionsList = new App.CollectionListView({ collection: new App.CollectionList() });
		
		this.router = new App.Router();
		Backbone.history.start({ root: '/admin' });
		
		$.ajax({
			url: "/version",
		})
		.done(function(res) {
			$('.navbar-brand').html('Tiedot v' + res);
		});
	},
	
	onLoadDocFormSubmit: function(e) {
		e.preventDefault();
		
		var doc = $('.load-doc .doc').val().toString().trim();
		
		if (!doc || !doc.match(/^[a-zA-Z]+\/[0-9]+$/)) {
			alert('Missing or invalid ID.');
			return false;
		}
		
		var segments = doc.split('/');
		this.router.navigate('docs/' + segments[0] + '/' + segments[1], { trigger: true });
		
		return false;
	},
	
	notify: function(type, msg) {
		$('#main').prepend('<div class="alert alert-' + type + ' alert-dismissable fade in"><button type="button" class="close" data-dismiss="alert" aria-hidden="true">&times;</button>' + msg + '</div>');
		$(".alert").alert();
		
		setTimeout(function() {
			$(".alert").alert('close')
		}, 4000);
	}
});

App.Router = Backbone.Router.extend({

	routes: {
		'': 'index',
		'cols/:name': 'collectionByName',
		'docs/new/:col': 'newDoc',
		'docs/:col/:id': 'docById'
	},
	
	index: function() {
		$("#app").html('');
	},
		
	collectionByName: function(name) {
		var collection = new App.CollectionView({ id: name, model: new App.Collection({ id: name }), collection: new App.DocumentList() });
	},

	newDoc: function(col) {
		var documentView = new App.DocumentView({ col: col, model: new App.Document() });
	},
	
	docById: function(col, id) {
		var documentView = new App.DocumentView({ id: id, col: col, model: new App.Document({ id: id }) });
	}

});

App.Modal = function () {

    var modalId = '#modal';
	var modalContent = '#modal-content';
    var closeCallback;

    var showModal = function (content, callback, onClose) {
        $(modalContent).html(content);
        $(modalId).modal();

        if (typeof callback === 'function') {
            callback.apply($(modalId));
        }
        if (typeof onClose === 'function') {
            closeCallback = onClose;
        } else {
            closeCallback = null;
        }
    };

    var hideModal = function (callback) {
        $(modalContent).html('');
		$(modalId).modal('hide');

        setTimeout(function () {
          if (typeof callback === 'function') {
            callback.apply($(modalId));
          }
          if (typeof closeCallback === 'function') {
            closeCallback.apply($(modalId));
            closeCallback = null;
          }
        }, 500);
    };

    return {

        init: function () {
			window.dispatcher.on('modal:open', showModal);
			window.dispatcher.on('modal:close', hideModal);
        }

    };
};

$(function() {
	window.dispatcher = _.clone(Backbone.Events);

	tiedotApp = new App.AppView();
});