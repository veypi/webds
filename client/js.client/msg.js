/*eslint-disable block-scoped-var, id-length, no-control-regex, no-magic-numbers, no-prototype-builtins, no-redeclare, no-shadow, no-var, sort-vars*/
'use strict'

var $protobuf = require('protobufjs/light')

var $root = ($protobuf.roots['default'] || ($protobuf.roots['default'] = new $protobuf.Root()))
  .addJSON({
    message: {
      nested: {
        message: {
          fields: {
            type: {
              type: 'typ',
              id: 1
            },
            tag: {
              type: 'uint64',
              id: 2
            },
            source: {
              type: 'string',
              id: 3
            },
            target: {
              type: 'string',
              id: 4
            },
            data: {
              type: 'bytes',
              id: 5
            },
            unixTime: {
              type: 'int64',
              id: 6
            }
          },
          nested: {
            typ: {
              values: {
                Bytes: 0,
                String: 1,
                Int: 2,
                Bool: 3,
                JSON: 4
              }
            }
          }
        }
      }
    }
  })

module.exports = $root
