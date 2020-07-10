import protoRoot from './msg'

const Message = protoRoot.lookup('message.message')
const websocketByteMessageType = 0
const websocketStringMessageType = 1
const websocketIntMessageType = 2
const websocketBoolMessageType = 3
const websocketJSONMessageType = 4

const typeErr = new Error('type error')
let msgCounter = 0

// utils
function isNumber(obj) {
  return !isNaN(obj - 0) && obj !== null && obj !== '' && obj !== false
}

function isString(obj) {
  return Object.prototype.toString.call(obj) === '[object String]'
}

function isBoolean(obj) {
  return typeof obj === 'boolean' || (typeof obj === 'object' && typeof obj.valueOf() === 'boolean')
}

function isJSON(obj) {
  return typeof obj === 'object'
}

/**
 * @return {string}
 */
function TypeOfTopic(t) {
  t = new Topic(t)
  let f1 = t.FirstFragment()
  if (f1 === SysTopic) {
    return f1
  } else if (f1 === InnerTopic) {
    return f1
  } else {
    return PublicTopic
  }
}

class Topic {
  constructor(t) {
    if (!isString(t)) {
      throw typeErr
    }
    if (t.length > 0 && t[0] === '/') {
      this.abs = t
    } else {
      this.abs = '/' + t
    }
  }

  String() {
    return this.abs
  }

  len() {
    return this.abs.length
  }

  Fragment(count) {
    if (!isNumber(count)) {
      throw typeErr
    }
    let res = ''
    let tempCount = -1
    for (let i = 0; i < this.len(); i++) {
      if (this.abs[i] === '/') {
        if (tempCount === count) {
          break
        }
        tempCount++
      } else if (tempCount === count) {
        res += this.abs[i]
      }
    }
    return res
  }

  FirstFragment() {
    return this.Fragment(0)
  }

  Since(count) {
    if (!isNumber(count)) {
      throw typeErr
    }
    let index = 0
    let tempCount = -1
    for (let i = 0; i < this.len(); i++) {
      if (this.abs[i] === '/') {
        if (tempCount === count) {
          break
        }
        tempCount++
        index = i
      }
    }
    return this.abs.substr(index)
  }
}

const PublicTopic = ''
const SysTopic = 'sys'
const InnerTopic = 'inner'
const TopicSubscribe = new Topic('/sys/topic/subscribe')
const TopicCancel = new Topic('/sys/topic/cancel')
const TopicCancelAll = new Topic('/sys/topic/cancel_all')
const TopicSysLog = new Topic('/sys/base/log')
const TopicAuth = new Topic('/sys/base/auth')

//
class Ws {
  constructor(endpoint, protocols) {
    let _this = this
    // events listeners
    this.isReady = false
    this.connectListeners = []
    this.disconnectListeners = []
    this.messageListeners = {}
    if (!window['WebSocket']) {
      return
    }
    if (endpoint.indexOf('ws') === -1) {
      endpoint = 'ws://' + endpoint
    }
    if (protocols != null && protocols.length > 0) {
      this.conn = new WebSocket(endpoint, protocols)
    } else {
      this.conn = new WebSocket(endpoint)
    }
    this.conn.binaryType = 'blob'
    this.conn.onopen = function (evt) {
      _this.isReady = true
      for (let i in _this.messageListeners) {
        _this._subscribe(i)
      }
      // _this.fireConnect()
      return null
    }
    this.conn.onclose = function (evt) {
      _this.fireDisconnect()
      return null
    }
    this.conn.onmessage = function (evt) {
      if (evt.data instanceof Blob) {
        let reader = new FileReader()
        reader.onload = () => {
          _this.messageReceivedFromConn(Message.decode(new Uint8Array(reader.result)))
        }
        reader.readAsArrayBuffer(evt.data)
      } else {
        _this.messageReceivedFromConn(Message.decode(evt.data))
      }
    }
  }

  //
  // messages
  // 格式: prefix(n)type(1)random_tag(4)source_idx(4)target_topic;msg
  _msg(topic, websocketMessageType, dataMessage) {
    msgCounter++
    return Message.encode({
      'type': websocketMessageType,
      'tag': msgCounter,
      'source': '',
      'target': topic.String(),
      'data': new Uint8Array(dataMessage),
      'unix_time': Math.round(new Date().getTime() / 1000)
    }).finish()
  }

  encodeMessage(event, data) {
    let m = ''
    let t = 0
    if (isNumber(data)) {
      t = websocketIntMessageType
      m = data.toString()
    } else if (isBoolean(data)) {
      t = websocketBoolMessageType
      if (data) {
        m = String.fromCharCode(0x01)
      } else {
        m = String.fromCharCode(0x00)
      }
    } else if (isString(data)) {
      t = websocketStringMessageType
      m = data.toString()
    } else if (isJSON(data)) {
      // TODO:: json proto3
      t = websocketJSONMessageType
      m = JSON.stringify(data)
    } else if (data !== null && typeof data !== 'undefined') {
      // if it has a second parameter but it's not a type we know, then fire this:
      console.log(
        'unsupported type of input argument passed, try to not include this argument to the \'Emit\''
      )
    }
    return this._msg(event, t, m)
  }

  decodeMessage(m) {
    let websocketMessageType = m.type
    let theMessage = Uint8ArrayToString(m.data)
    if (websocketMessageType === websocketIntMessageType) {
      return parseInt(theMessage)
    } else if (websocketMessageType === websocketBoolMessageType) {
      return theMessage === '1'
    } else if (websocketMessageType === websocketStringMessageType) {
      return theMessage
    } else if (websocketMessageType === websocketJSONMessageType) {
      return JSON.parse(theMessage)
    } else if (websocketMessageType === websocketByteMessageType) {
      return theMessage
    } else {
      return null // invalid
    }
  }

  //
  // Ws Events
  messageReceivedFromConn(message) {
    let topic = new Topic(message.target)
    let data = this.decodeMessage(message)
    if (topic !== '') {
      if (topic.FirstFragment() === SysTopic) {
        if (topic.String() === TopicAuth.String()) {
          if (data === 'pass') {
            this.fireConnect()
          } else if (data === 'duplicated conn') {
          } else {
            console.log(data)
          }
        } else if (topic.String() === TopicSysLog.String()) {
          console.log(data)
        }
      }
      // it's a custom message
      this.fireMessage(topic, data)
    }
  }

  OnConnect(fn) {
    if (this.isReady) {
      fn()
    }
    this.connectListeners.push(fn)
  }

  fireConnect() {
    for (let i = 0; i < this.connectListeners.length; i++) {
      this.connectListeners[i]()
    }
  }

  OnDisconnect(fn) {
    this.disconnectListeners.push(fn)
  }

  fireDisconnect() {
    for (let i = 0; i < this.disconnectListeners.length; i++) {
      this.disconnectListeners[i]()
    }
  }

  Subscribe(topic, cb, only = true) {
    let t = new Topic(topic)
    let event = t.String()
    this._subscribe(event)
    if (only) {
      this.messageListeners[event] = [cb]
      return
    }
    if (this.messageListeners[event] === null || this.messageListeners[event] === undefined) {
      this.messageListeners[event] = []
    }
    this.messageListeners[event].push(cb)
  }

  _subscribe(t) {
    if (this.isReady && TypeOfTopic(t) !== SysTopic) {
      this.pub(TopicSubscribe, t)
    }
  }

  fireMessage(event, message) {
    event = event.String()
    for (let key in this.messageListeners) {
      if (this.messageListeners.hasOwnProperty(key)) {
        if (key === event) {
          for (let i = 0; i < this.messageListeners[key].length; i++) {
            this.messageListeners[key][i](message)
          }
        }
      }
    }
  }

  // Ws Actions
  Disconnect() {
    this.conn.close()
  }

  write(msg) {
    this.conn.send(msg)
  }

  Publisher(topic) {
    let t = new Topic(topic)
    return (data) => {
      this.pub(t, data)
    }
  }

  Pub(topic, data) {
    this.pub(new Topic(topic), data)
  }

  pub(t, data) {
    this.write(this.encodeMessage(t, data))
  }
}

function randomString(len) {
  len = len || 32
  let $chars = 'ABCDEFGHJKMNPQRSTWXYZabcdefhijkmnprstwxyz2345678'
  /****默认去掉了容易混淆的字符oOLl,9gq,Vv,Uu,I1****/
  let maxPos = $chars.length
  let pwd = ''
  for (let i = 0; i < len; i++) {
    pwd += $chars.charAt(Math.floor(Math.random() * maxPos))
  }
  return pwd
}

function int32ToBytesStr(i) {
  return String.fromCharCode(i >> 24, i >> 16, i >> 8, i)
}

function bytesToInt32(b) {
  return (
    b.charCodeAt(3) | (b.charCodeAt(2) << 8) | (b.charCodeAt(1) << 16) | (b.charCodeAt(0) << 24)
  )
}

function Uint8ArrayToString(fileData) {
  var dataString = ''
  for (var i = 0; i < fileData.length; i++) {
    dataString += String.fromCharCode(fileData[i])
  }
  return dataString
}

export default Ws
// let script = document.createElement('script');
// script.src = "file:///home/light/test/ws.js";
// document.getElementsByTagName('head')[0].appendChild(script);
