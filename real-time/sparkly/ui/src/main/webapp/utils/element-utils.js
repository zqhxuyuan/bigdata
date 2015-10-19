Element.prototype.removeAllListeners = function() {
	var cloned = this.cloneNode(true);
    this.parentNode.replaceChild(cloned, this);
};