var tiedotApp;

App.AppView = Backbone.View.extend({
	el: $("#app"),
	router: {},
	
	initialize: function() {
		this.modal = App.Modal();
		this.modal.init();

		$('.load-doc').on('submit', this.onLoadDocFormSubmit);
				
		this.router = new App.Router();
		this.queryBox = new App.QueryBoxView({ el: $('#query-box') });
		
		$.ajax({
			url: "/version",
		})
		.done(function(res) {
			$('.navbar-brand').html('Tiedot (API version ' + res + ')');
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
	
	notify: function(type, msg, time) {
		$('#main').prepend('<div class="alert alert-' + type + ' alert-dismissable fade in"><button type="button" class="close" data-dismiss="alert" aria-hidden="true">&times;</button>' + msg + '</div>');
		$(".alert").alert();
		
		setTimeout(function() {
			$(".alert").alert('close')
		}, time ? time : 4000);
	}
});

$(function() {
	window.dispatcher = _.clone(Backbone.Events);
	
	tiedotApp = new App.AppView();
	Backbone.history.start({ root: App.root });
});