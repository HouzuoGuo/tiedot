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