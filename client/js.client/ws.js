var websocketStringMessageType = 0
var websocketIntMessageType = 1
var websocketBoolMessageType = 2
var websocketByteMessageType = 3
var websocketJSONMessageType = 4
var websocketMessagePrefix = 'ws:'
var websocketMessageSeparator = ';'
var websocketMessagePrefixLen = websocketMessagePrefix.length
var websocketMessageSeparatorLen = websocketMessageSeparator.length
var websocketMessagePrefixAndSepIdx = websocketMessagePrefixLen + websocketMessageSeparatorLen - 1
var websocketMessagePrefixIdx = websocketMessagePrefixLen - 1
var websocketMessageSeparatorIdx = websocketMessageSeparatorLen - 1

//
class Ws {
  constructor(endpoint, protocols) {
    var _this = this
    // events listeners
    this.connectListeners = []
    this.disconnectListeners = []
    this.nativeMessageListeners = []
    this.messageListeners = {}
    if (!window['WebSocket']) {
      return
    }
    if (endpoint.indexOf('ws') == -1) {
      endpoint = 'ws://' + endpoint
    }
    if (protocols != null && protocols.length > 0) {
      this.conn = new WebSocket(endpoint, protocols)
    } else {
      this.conn = new WebSocket(endpoint)
    }
    this.conn.onopen = function(evt) {
      _this.fireConnect()
      _this.isReady = true
      return null
    }
    this.conn.onclose = function(evt) {
      _this.fireDisconnect()
      return null
    }
    this.conn.onmessage = function(evt) {
      _this.messageReceivedFromConn(evt)
    }
  }
  // utils
  isNumber(obj) {
    return !isNaN(obj - 0) && obj !== null && obj !== '' && obj !== false
  }
  isString(obj) {
    return Object.prototype.toString.call(obj) === '[object String]'
  }
  isBoolean(obj) {
    return (
      typeof obj === 'boolean' || (typeof obj === 'object' && typeof obj.valueOf() === 'boolean')
    )
  }
  isJSON(obj) {
    return typeof obj === 'object'
  }
  //
  // messages
  _msg(event, websocketMessageType, dataMessage) {
    return (
      websocketMessagePrefix +
      event +
      websocketMessageSeparator +
      String(websocketMessageType) +
      websocketMessageSeparator +
      dataMessage
    )
  }
  encodeMessage(event, data) {
    var m = ''
    var t = 0
    if (this.isNumber(data)) {
      t = websocketIntMessageType
      m = data.toString()
    } else if (this.isBoolean(data)) {
      t = websocketBoolMessageType
      m = data.toString()
    } else if (this.isString(data)) {
      t = websocketStringMessageType
      m = data.toString()
    } else if (this.isJSON(data)) {
      // propably json-object
      t = websocketJSONMessageType
      m = JSON.stringify(data)
    } else if (data !== null && typeof data !== 'undefined') {
      // if it has a second parameter but it's not a type we know, then fire this:
      console.log(
        "unsupported type of input argument passed, try to not include this argument to the 'Emit'"
      )
    }
    if (event === 'transfer') {
      t = websocketByteMessageType
    }
    return this._msg(event, t, m)
  }
  decodeMessage(event, websocketMessage) {
    var skipLen = websocketMessagePrefixLen + websocketMessageSeparatorLen + event.length + 2
    if (websocketMessage.length < skipLen + 1) {
      return null
    }
    var websocketMessageType = parseInt(websocketMessage.charAt(skipLen - 2))
    var theMessage = websocketMessage.substring(skipLen, websocketMessage.length)
    if (websocketMessageType == websocketIntMessageType) {
      return parseInt(theMessage)
    } else if (websocketMessageType == websocketBoolMessageType) {
      return Boolean(theMessage)
    } else if (websocketMessageType == websocketStringMessageType) {
      return theMessage
    } else if (websocketMessageType == websocketJSONMessageType) {
      return JSON.parse(theMessage)
    } else if (websocketMessageType == websocketByteMessageType) {
      return JSON.parse(theMessage)
    } else {
      return null // invalid
    }
  }
  getWebsocketCustomEvent(websocketMessage) {
    if (websocketMessage.length < websocketMessagePrefixAndSepIdx) {
      return ''
    }
    var s = websocketMessage.substring(websocketMessagePrefixAndSepIdx, websocketMessage.length)
    var evt = s.substring(0, s.indexOf(websocketMessageSeparator))
    return evt
  }
  getCustomMessage(event, websocketMessage) {
    var eventIdx = websocketMessage.indexOf(event + websocketMessageSeparator)
    var s = websocketMessage.substring(
      eventIdx + event.length + websocketMessageSeparator.length + 2,
      websocketMessage.length
    )
    return s
  }
  //
  // Ws Events
  // messageReceivedFromConn this is the func which decides
  // if it's a native websocket message or a custom qws message
  // if native message then calls the fireNativeMessage
  // else calls the fireMessage
  //
  // remember iris gives you the freedom of native websocket messages if you don't want to use this client side at all.
  messageReceivedFromConn(evt) {
    // check if qws message
    var message = evt.data
    if (message.indexOf(websocketMessagePrefix) != -1) {
      var event_1 = this.getWebsocketCustomEvent(message)
      if (event_1 != '') {
        // it's a custom message
        this.fireMessage(event_1, this.decodeMessage(event_1, message))
        // this.fireMessage(event_1, this.getCustomMessage(event_1, message));
        return
      }
    }
    // it's a native websocket message
    this.fireNativeMessage(message)
  }
  OnConnect(fn) {
    if (this.isReady) {
      fn()
    }
    this.connectListeners.push(fn)
  }
  fireConnect() {
    for (var i = 0; i < this.connectListeners.length; i++) {
      this.connectListeners[i]()
    }
  }
  OnDisconnect(fn) {
    this.disconnectListeners.push(fn)
  }
  fireDisconnect() {
    for (var i = 0; i < this.disconnectListeners.length; i++) {
      this.disconnectListeners[i]()
    }
  }
  OnMessage(cb) {
    this.nativeMessageListeners.push(cb)
  }
  fireNativeMessage(websocketMessage) {
    for (var i = 0; i < this.nativeMessageListeners.length; i++) {
      this.nativeMessageListeners[i](websocketMessage)
    }
  }
  On(event, cb, only = true) {
    if (only) {
      this.messageListeners[event] = [cb]
      return
    }
    if (this.messageListeners[event] === null || this.messageListeners[event] === undefined) {
      this.messageListeners[event] = []
    }
    this.messageListeners[event].push(cb)
  }
  fireMessage(event, message) {
    for (var key in this.messageListeners) {
      if (this.messageListeners.hasOwnProperty(key)) {
        if (key == event) {
          for (var i = 0; i < this.messageListeners[key].length; i++) {
            this.messageListeners[key][i](message)
          }
        }
      }
    }
  }
  //
  // Ws Actions
  Disconnect() {
    this.conn.close()
  }
  // EmitMessage sends a native websocket message
  EmitMessage(websocketMessage) {
    this.conn.send(websocketMessage)
  }
  // Emit sends an custom websocket message
  Emit(event, data) {
    if (event === 'transfer' && this.isJSON(data)) {
      data = JSON.stringify(data)
    }
    var messageStr = this.encodeMessage(event, data)
    this.EmitMessage(messageStr)
  }
}

export default Ws
// var script = document.createElement('script');
// script.src = "file:///home/light/test/ws.js";
// document.getElementsByTagName('head')[0].appendChild(script);
