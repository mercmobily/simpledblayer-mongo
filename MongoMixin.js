/*
Copyright (C) 2013 Tony Mobily

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/



var 
  dummy

, declare = require('simpledeclare')
, mongoWrapper = require('mongowrapper')

, ObjectId = mongoWrapper.ObjectId
, checkObjectId = mongoWrapper.checkObjectId

;


var MongoMixin = declare( null, {

  projectionHash: {},

  constructor: function( table, fields ){

    var self = this;

    // Make up the projectionHash, which is used in pretty much every query 
    self.projectionHash = {};
    Object.keys( fields ).forEach( function( field ) {
       self.projectionHash[ field ] = true;
    });

    // Make sure that I have `_id: false` in the projection hash (used in all finds)
    // if `_id` is not explicitely defined in the schema.
    // in "inclusive projections" in mongoDb, _id is added automatically and it needs to be
    // explicitely excluded (it is, in fact, the ONLY field that can be excluded in an inclusive projection)
    // FIXME: Taken this out of the picture, as _id is always important for the requester
    // if( typeof( fields._id ) === 'undefined' ) this.projectionHash._id = false ;

    // Create self.collection, used by every single query
    self.collection = self.db.collection( self.table );

  },

  // The default id maker available as an object method
  makeId: function( object, cb ){
    MongoSchemaMixin.makeId( object, cb );
  },


  _makeMongoParameters: function( filters ){

    var selector = {};

    if( typeof( filters.conditions ) !== 'undefined' && filters.conditions !== null ){
      selector[ '$and' ] =  [];
      selector[ '$or' ] =  [];

      Object.keys( filters.conditions ).forEach( function( condition ){
        console.log( condition );

        // Sets the mongo condition
        var mongoOperand = '$and';
        if( condition === 'or' ) mongoOperand = '$or';      
 
        Object.keys( filters.conditions[ condition ]).forEach( function( field ){
          var fieldObject = filters.conditions[ condition ][ field ];
          var item = { };
          item[ field ] = {};

          var v = fieldObject.value;

          switch( fieldObject.type ){
            case 'lt':
              item[ field ] = { $lt: v };
            break;

            case 'lte':
              item[ field ] = { $lte: v };
            break;

            case 'gt':
              item[ field ] = { $gt: v };
            break;

            case 'gte':
              item[ field ] = { $gte: v };
            break;

            case 'is':
            case 'eq':
              item[ field ] = v;
            break;

            case 'startsWith':
              item[ field ] = { $regex: new RegExp('^' + v + '.*' ) };
            case 'startWith':
            break;

            case 'contain':
              item[ field ] = { $regex: new RegExp( v ) };
            case 'contains':
            break;

            case 'endsWith':
              item[ field ] = { $regex: new RegExp('.*' + v + '$' ) };
            case 'endWith':
            break;

            default:
              throw( new Error("Field type unknown: " + fieldObject.type ) );
            break;
          }
         
          // Finally, push down the item!
          selector[ mongoOperand ].push( item );
        });

 
      });

      // Clean up selector, as Mongo doesn't like empty arrays for selectors
      if( selector[ '$and' ].length == 0 ) delete selector[ '$and' ];
      if( selector[ '$or' ].length == 0 ) delete selector[ '$or' ];

    };    
 
    // console.log("SELECTOR:");
    // console.log( require('util').inspect( selector, { depth: 10 }  ));

    return { querySelector: selector, sortHash: filters.sort };
  }, 

  select: function( filters, options, cb ){

    var self = this;
    var saneRanges;

    // Usual drill
    if( typeof( cb ) === 'undefined' ){
      cb = options;
      options = {}
    } else if( typeof( options ) !== 'object' || options === null ){
      throw( new Error("The options parameter must be a non-null object") );
    }

    // Actually run the query 
    var mongoParameters = this._makeMongoParameters( filters );
    var cursor = self.collection.find( mongoParameters.querySelector, self.projectionHash );

    // Sanitise ranges
    saneRanges = self.sanitizeRanges( filters.ranges );

    // Skipping/limiting according to ranges/limits
    if( saneRanges.from != 0 )  cursor.skip( saneRanges.from );
    if( saneRanges.limit != 0 ) cursor.limit( saneRanges.limit );

    // Sort the query
    cursor.sort( mongoParameters.sortHash , function( err ){
      if( err ){
        next( err );
      } else {


        if( options.useCursor ){

          cb( null, {

            next: function( done ){

              cursor.nextObject( function( err, obj) {
                if( err ){
                  done( err );
                } else {

                  // If options.delete is on, then remove a field straight after fetching it
                  if( options.delete && obj !== null ){
                    self.collection.remove( { _id: obj._id }, function( err, howMany ){
                      if( err ){
                        done( err );
                      } else {
                        done( null, obj );
                      }
                    });
                  } else {
                     done( null, obj );
                  }
                }
              });
            },

            rewind: function( done ){
              if( options.delete ){
                done( new Error("Cannot rewind a cursor with `delete` option on") );
              } else {
                cursor.rewind();
                done( null );
              }
            },
            close: function( done ){
              cursor.close( done );
            }
          });

        } else {

          cursor.count( { applySkipLimit: true }, function( err, total ){
            if( err ){
              cb( err );
            } else {

              cursor.toArray( function( err, queryDocs ){
                if( err ){
                 cb( err );
                } else {
                  queryDocs.total = total;

                  if( options.delete ){
                    
                    self.collection.remove( mongoParameters.querySelector, { multi: true }, function( err ){
                      if( err ){
                        cb( err );
                      } else {
                        cb( null, queryDocs );
                      }
                    });
                  }
                };
              });

            };
          })

        }
      }
    });
       
  },



  update: function( filters, record, options, cb ){

    var self = this;
    var unsetObject = {};
    var updateOptions = { multi: false };

    // Usual drill
    if( typeof( cb ) === 'undefined' ){
      cb = options;
      options = {}
    } else if( typeof( options ) !== 'object' || options === null ){
      throw( new Error("The options parameter must be a non-null object") );
    }

    // if `options.deleteUnsetFields`, Unset any value that is not actually set but IS in the schema,
    // so that partial PUTs will "overwrite" whole objects rather than
    // just overwriting fields that are _actually_ present in `body`
    if( options.deleteUnsetFields ){
      Object.keys( self.fields ).forEach( function( i ){
         if( typeof( record[ i ] ) === 'undefined' ) unsetObject[ i ] = 1;
      });
    }

    // Allow multiple updates
    if( options.multi ){
      updateOptions.multi = true;
    }

    // Run the query
    var mongoParameters = this._makeMongoParameters( filters );
    self.collection.update( mongoParameters.querySelector, { $set: record, $unset: unsetObject }, updateOptions, function( err, numberUpdated ) {
      if( err ){
        cb( err, null );
      } else {
        cb( null, numberUpdated );
      }
    });

  },


  insert: function( record, options, cb ){

    var self = this;

    // Usual drill
    if( typeof( cb ) === 'undefined' ){
      cb = options;
      options = {}
    } else if( typeof( options ) !== 'object' || options === null ){
      throw( new Error("The options parameter must be a non-null object") );
    }

    // Set the record ID to keep Mongo happy and make
    // subsequent search easier. 
    if( typeof( record._id ) === 'undefined' ) record._id  = ObjectId();

    // Actually run the insert
    self.collection.insert( record, function( err ){
      if( err ) {
        cb( err );
      } else {
        if( ! options.returnRecord ){
          cb( null, null );
        } else {
          self.collection.findOne( { _id: record._id }, self.projectionHash, cb );
        }
      }
    });

  },

  'delete': function( filters, options, cb ){

    var self = this;

    var deleteOptions = { single: true };

    // Usual drill
    if( typeof( cb ) === 'undefined' ){
      cb = options;
      options = {}
    } else if( typeof( options ) !== 'object' || options === null ){
      throw( new Error("The options parameter must be a non-null object") );
    }

    if( options.multi ){
      deleteOptions.single = false;
    }

    // Run the query
    var mongoParameters = this._makeMongoParameters( filters );
    self.collection.remove( mongoParameters.querySelector, deleteOptions, cb );
  },


});

// The default id maker
MongoMixin.makeId = function( object, cb ){
  cb( null, ObjectId() );
},

exports = module.exports = MongoMixin;


