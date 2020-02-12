const websocketStringMessageType = 0;
const websocketIntMessageType = 1;
const websocketBoolMessageType = 2;
const websocketByteMessageType = 3;
const websocketJSONMessageType = 4;
const websocketMessagePrefix = 'ws:';
const websocketMessageSeparator = ';';
const websocketMessagePrefixLen = websocketMessagePrefix.length;
const websocketMessageSeparatorLen = websocketMessageSeparator.length;
const websocketMessagePrefixAndSepIdx = websocketMessagePrefixLen + websocketMessageSeparatorLen - 1;
const websocketMessagePrefixIdx = websocketMessagePrefixLen - 1;
const websocketMessageSeparatorIdx = websocketMessageSeparatorLen - 1;
const typeErr = new Error("type error")
const NotAllowedTopic = new Error("this topic is not allowed to subscribe/publish")
const UnformedMsg = new Error("unformed msg")
const PublicTopic = ""
const SysTopic = "sys"
const InnerTopic = "inner"
const TopicSubscribe = new Topic("/sys/subscribe")
const TopicCancel = new Topic("/sys/cancel")
const TopicCancelAll = new Topic("/sys/cancel_all")
const TopicSysLog = new Topic("/sys/log")
const TopicAuth = new Topic('/sys/auth')

// utils
function isNumber(obj) {
    return !isNaN(obj - 0) && obj !== null && obj !== '' && obj !== false
}

function isString(obj) {
    return Object.prototype.toString.call(obj) === '[object String]'
}

function isBoolean(obj) {
    return (
        typeof obj === 'boolean' || (typeof obj === 'object' && typeof obj.valueOf() === 'boolean')
    )
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
        this.conn.onopen = function (evt) {
            _this.fireConnect()
            _this.isReady = true
            for (let i in _this.messageListeners) {
                _this._subscribe(i)
            }
            return null
        }
        this.conn.onclose = function (evt) {
            _this.fireDisconnect()
            return null
        }
        this.conn.onmessage = function (evt) {
            _this.messageReceivedFromConn(evt)
        }
    }


    //
    // messages
    _msg(topic, websocketMessageType, dataMessage) {
        return (
            websocketMessagePrefix +
            topic.String() +
            websocketMessageSeparator +
            String(websocketMessageType) +
            websocketMessageSeparator +
            dataMessage
        )
    }

    encodeMessage(event, data) {
        let m = ''
        let t = 0
        if (isNumber(data)) {
            t = websocketIntMessageType
            m = data.toString()
        } else if (isBoolean(data)) {
            t = websocketBoolMessageType
            m = data.toString()
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
                "unsupported type of input argument passed, try to not include this argument to the 'Emit'"
            )
        }
        return this._msg(event, t, m)
    }

    decodeMessage(event, websocketMessage) {
        let skipLen = websocketMessagePrefixLen + websocketMessageSeparatorLen + event.len() + 2
        if (websocketMessage.length < skipLen + 1) {
            return null
        }
        let websocketMessageType = parseInt(websocketMessage.charAt(skipLen - 2))
        let theMessage = websocketMessage.substring(skipLen, websocketMessage.length)
        if (websocketMessageType === websocketIntMessageType) {
            return parseInt(theMessage)
        } else if (websocketMessageType === websocketBoolMessageType) {
            return Boolean(theMessage)
        } else if (websocketMessageType === websocketStringMessageType) {
            return theMessage
        } else if (websocketMessageType === websocketJSONMessageType) {
            return JSON.parse(theMessage)
        } else if (websocketMessageType === websocketByteMessageType) {
            return JSON.parse(theMessage)
        } else {
            return null // invalid
        }
    }

    getWebsocketCustomEvent(websocketMessage) {
        if (websocketMessage.length < websocketMessagePrefixAndSepIdx) {
            return ''
        }
        let s = websocketMessage.substring(websocketMessagePrefixAndSepIdx, websocketMessage.length)
        return new Topic(s.substring(0, s.indexOf(websocketMessageSeparator)))
    }

    //
    // Ws Events
    messageReceivedFromConn(evt) {
        // check if qws message
        let message = evt.data
        if (message.indexOf(websocketMessagePrefix) !== -1) {
            let topic = this.getWebsocketCustomEvent(message)
            if (topic !== '') {
                // it's a custom message
                this.fireMessage(topic, this.decodeMessage(topic, message))
            }
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
        if (this.isReady) {
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
        this.write(this.encodeMessage(t), data)
    }
}

export default Ws
// let script = document.createElement('script');
// script.src = "file:///home/light/test/ws.js";
// document.getElementsByTagName('head')[0].appendChild(script);
