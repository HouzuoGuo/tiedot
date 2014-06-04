var app = {};

var app.Router = Backbone.Router.extend({

  routes: {
    "/": "index",
    "col/:name": "collectionByName",
    "doc/:id": "docById"
  },

  index: function() {
    
  },

  collectionByName: function(query, page) {
    
  }

  docById: function(query, page) {
    
  }

});

$(function(){
  new app.Router();
  Backbone.history.start({ root: '/admin' });
});