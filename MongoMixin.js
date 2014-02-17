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
, async = require('async')

, ObjectId = mongoWrapper.ObjectId
, checkObjectId = mongoWrapper.checkObjectId
;

var MongoMixin = declare( null, {

  _projectionHash: {},
  _searchableHash: {},
  _fieldsHash: {},

  constructor: function( table, options ){

    var self = this;

    // Make up the _***Hash variables, which are used widely
    // within the module
    self._projectionHash = {};
    self._searchableHash = {};
    self._fieldsHash = {};

    Object.keys( self.schema.structure ).forEach( function( field ) {
      var entry = self.schema.structure[ field ];
      self._fieldsHash[ field ] = true;
			if( ! entry.skipProjection ) self._projectionHash[ field ] = true;
      if( entry.searchable ) self._searchableHash[ field ] = true;
    });

    // Create self.collection, used by every single query
    self.collection = self.db.collection( self.table );

  },

  // The default id maker available as an object method
  makeId: function( object, cb ){
    MongoMixin.makeId( object, cb );
  },


  _makeMongoParameters: function( filters ){

    var self = this;

    var selector = {}, finalSelector = {};

    if( typeof( filters.conditions ) !== 'undefined' && filters.conditions !== null ){
      selector[ '$and' ] =  [];
      selector[ '$or' ] =  [];

      Object.keys( filters.conditions ).forEach( function( condition ){

        // Sets the mongo condition
        var mongoOperand = '$and';
        if( condition === 'or' ) mongoOperand = '$or';      
 
        filters.conditions[ condition ].forEach( function( fieldObject ){

          var field = fieldObject.field;
          var v = fieldObject.value;


          // If a search is attempted on a non-searchable field, will throw
          if( !self._searchableHash[ field ] ){
            throw( new Error("Field " + field + " is not searchable" ) );
          }

          /*
          if( self._searchableHash[ field ] && typeof( fieldObject.value ) === 'string' ){
            field = '__uc__' + field;
            v = v.toUpperCase();
          }
          */

          var item = { };
          item[ field ] = {};

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
              item[ field ] = new RegExp('^' + v + '.*' );
            case 'startWith':
            break;

            case 'contain':
            case 'contains':
              item[ field ] = new RegExp('.*' + v + '.*' );
            break;

            case 'endsWith':
              item[ field ] = new RegExp('.*' + v + '$' );
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

      // Assign the `finalSelector` variable. Note that the final result can be:
      // * Just $and conditions: { '$and': [ this, that, other ] }
      // * Just $or conditions : { '$or': [ this, that, other ] }
      // * $and conditions with $or: { '$and': [ this, that, other, { '$or': [ blah, bleh, bligh ] } ] }


      // No `$and` conditions...
      if( selector[ '$and' ].length === 0 ){
        // ...maybe there are `or` ones, which will get returned
        if( selector[ '$or' ].length !== 0 ) finalSelector[ '$or' ] = selector[ '$or' ];

      // There are `$and` conditions: assign them...
      } else {
        finalSelector[ '$and' ] = selector[ '$and' ];

        // ...and shove the `$or` ones in there as one of them
        if( selector[ '$or' ].length !== 0 ){
          finalSelector[ '$and' ].push( { '$or': selector[ '$or' ] } );
        }

      }
      // console.log( "FINAL SELECTOR" );        
      // console.log( require('util').inspect( finalSelector, { depth: 10 } ) );        

    };    

    // make sortHash
    // If field is searchable, swap field names for _uc_ equivalent so that
    // sorting happens regardless of upper or lower case
    var sortHash = {};
    for( var field  in filters.sort ) {
      // if( self._searchableHash[ field ] )  var finalField = '__uc__' + field; else finalField = field;
      sortHash[ field ] = filters.sort[ field ];
    }
    //console.log( "FINAL SORTHASH", self.table );        
    //console.log( require('util').inspect( sortHash, { depth: 10 } ) );        

    return { querySelector: finalSelector, sortHash: sortHash };
  }, 



  select: function( filters, options, cb ){

    var self = this;
    var saneRanges;

    // Usual drill
    if( typeof( cb ) === 'undefined' ){
      cb = options;
      options = {}
    } else if( typeof( options ) !== 'object' || options === null ){
      return cb( new Error("The options parameter must be a non-null object") );
    }

    // Make up parameters from the passed filters
    try {
      var mongoParameters = this._makeMongoParameters( filters );
    } catch( e ){
      return cb( e );
    }

    // Actually run the query 
    var cursor = self.collection.find( mongoParameters.querySelector, self._projectionHash );
    //console.log("FIND IN SELECT: ",  mongoParameters.querySelector, self._projectionHash );

    // Sanitise ranges. If it's a cursor query, or if the option skipHardLimitOnQueries is on,
    // then will pass true (that is, the skipHardLimitOnQueries parameter will be true )
    saneRanges = self.sanitizeRanges( filters.ranges, options.useCursor || options.skipHardLimitOnQueries );

    // Skipping/limiting according to ranges/limits
    if( saneRanges.from != 0 )  cursor.skip( saneRanges.from );
    if( saneRanges.limit != 0 ) cursor.limit( saneRanges.limit );

    // Sort the query
    cursor.sort( mongoParameters.sortHash , function( err ){
      if( err ){
        next( err );
      } else {

        if( options.useCursor ){

          cursor.count( function( err, grandTotal ){
            if( err ){
              cb( err );
            } else {

              cursor.count( { applySkipLimit: true }, function( err, total ){
                if( err ){
                  cb( err );
                } else {

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

                                if( typeof( self._fieldsHash._id ) === 'undefined' )  delete obj._id;

                                self.schema.validate( obj, function( err, obj, errors ){

                                  // If there is an error, end of story
                                  // If validation fails, call callback with self.SchemaError
                                  if( err ) return cb( err );
                                  //if( errors.length ) return cb( new self.SchemaError( { errors: errors } ) );

                                  done( null, obj );
                                });
                              }
                            });
                          } else {

                            if( obj !== null && typeof( self._fieldsHash._id ) === 'undefined' )  delete obj._id;

                            if( obj === null ) return done( null, obj );

                            self.schema.validate( obj, function( err, obj, errors ){

                              // If there is an error, end of story
                              // If validation fails, call callback with self.SchemaError
                              if( err ) return cb( err );
                              //if( errors.length ) return cb( new self.SchemaError( { errors: errors } ) );

                              done( null, obj );
                            });
                              
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
                  }, total, grandTotal );

                }
              });

            }
          });

        } else {

          cursor.toArray( function( err, queryDocs ){
            if( err ){
             cb( err );
            } else {

              cursor.count( function( err, grandTotal ){
                if( err ){
                  cb( err );
                } else {

                  cursor.count( { applySkipLimit: true }, function( err, total ){
                    if( err ){
                      cb( err );
                    } else {

                      // Cycle to work out the toDelete array _and_ get rid of the _id_
                      // from the resultset
                      var toDelete = [];
                      queryDocs.forEach( function( doc ){
                        if( options.delete ) toDelete.push( doc._id );
                        if( typeof( self._fieldsHash._id ) === 'undefined' ) delete doc._id;
                      });

                      // If it was a delete, delete each record
                      // Note that there is no check whether the delete worked or not
                      if( options.delete ){
                        toDelete.forEach( function( _id ){
                          self.collection.remove( { _id: _id }, function( err ){ } );
                        });
                      }

                      var changeFunctions = [];
                      // Validate each doc, running a validate function for each one of them in parallel
                      queryDocs.forEach( function( doc, index ){

                        changeFunctions.push( function( callback ){
                          self.schema.validate( doc, function( err, validatedDoc, errors ){
                            if( err ){
                              callback( err );
                            } else {

                              //if( errors.length ) return cb( new self.SchemaError( { errors: errors } ) );
                              queryDocs[ index ] = validatedDoc;
                               
                              callback( null );
                            }
                          });
                        });
                      }); 
                      async.parallel( changeFunctions, function( err ){
                        if( err ) return cb( err );

                        // That's all!
                        cb( null, queryDocs, total, grandTotal );
                      });

                    };
                  });

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
    var recordToBeWritten = {};

    // Validate the record against the schema
    self.schema.validate( record, function( err, record, errors ){

      // If there is an error, end of story
      // If validation fails, call callback with self.SchemaError
      if( err ) return cb( err );
      if( errors.length ) return cb( new self.SchemaError( { errors: errors } ) );

      // Usual drill
      if( typeof( cb ) === 'undefined' ){
        cb = options;
        options = {}
      } else if( typeof( options ) !== 'object' || options === null ){
        return cb( new Error("The options parameter must be a non-null object") );
      }

      // Copy record over, only for existing fields
      for( var k in record ){
        if( typeof( self._fieldsHash[ k ] ) !== 'undefined' && k !== '_id' ) recordToBeWritten[ k ] = record[ k ];
      }

    /*
    // Sets the case-insensitive fields
    Object.keys( self._searchableHash ).forEach( function( fieldName ){
      if( self._searchableHash[ fieldName ] ){
        if( typeof( recordToBeWritten[ fieldName ] ) === 'string' ){
          recordToBeWritten[ '__uc__' + fieldName ] = recordToBeWritten[ fieldName ].toUpperCase();
        }
      }
    });
    */

      // If `options.deleteUnsetFields`, Unset any value that is not actually set but IS in the schema,
      // so that partial PUTs will "overwrite" whole objects rather than
      // just overwriting fields that are _actually_ present in `body`
      if( options.deleteUnsetFields ){
        Object.keys( self._fieldsHash ).forEach( function( i ){
           if( typeof( recordToBeWritten[ i ] ) === 'undefined' && i !== '_id' ) unsetObject[ i ] = 1;
        });
      }

      // Make up parameters from the passed filters
      try {
        var mongoParameters = self._makeMongoParameters( filters );
      } catch( e ){
        return cb( e );
      }

      // If options.multi is off, then use findAndModify which will accept sort
      if( !options.multi ){
        self.collection.findAndModify( mongoParameters.querySelector, mongoParameters.sortHash, { $set: recordToBeWritten, $unset: unsetObject }, function( err, doc ){
          if( err ){
            cb( err );
          } else {

            if( doc ){
              cb( null, 1 );
            } else {
              cb( null, 0 );
            }
          }
        });

      // If options.multi is on, then "sorting" doesn't make sense, it will just use mongo's "update"
      } else {

        // Run the query
        self.collection.update( mongoParameters.querySelector, { $set: recordToBeWritten, $unset: unsetObject }, { multi: true }, cb );
      }
    });

  },


  _makeAutoLoad: function( layer, record, childTable, currentObject, v, cb ){

    var self = this;

    // If resultObject is null, then it's the "root" record: set resultObject to its
    // _autoLoad key
    //if( resultObject === null ){


    // If called with just 4 parameters, it's the first time -- initialise currentObject and v
    if( typeof( currentObject ) === 'function' ){
      cb = currentObject;

      v = {};
      v.resultObject = [];

      currentObject = v.resultObject;
      console.log("MAKEAUTOLOAD CALLED FOR ", layer.table, "WILL HAVE TO LOAD RECORDS IN", childTable, "FOR RECORD", record );
    }

    //console.log("MAKEAUTOLOAD CALLED FOR ", layer.table, "WILL HAVE TO LOAD RECORDS IN", childTable, "FOR RECORD", record );

    var childTableData = layer.childrenTablesHash[ childTable ]; // Has `layer` and `nestedParams`
    var andConditionsArray = [];
       
    Object.keys( childTableData.nestedParams.join ).forEach( function( joinKey ){

      var joinValue = childTableData.nestedParams.join[ joinKey ];
      andConditionsArray.push( { field: joinKey, type: 'eq', value: record[ joinValue ] } );
    });

    //v.currentObject[ childTable ] = [];
    //console.log("ABOUT TO MAKE THE QUERY: ", andConditionsArray  );

    childTableData.layer.select( { conditions: { and: andConditionsArray } }, function( err, res, total ){
      if( err ) return cb( err );

      //console.log("SELECT CALLED");
      //console.log("FETCHED RECORDS: ", total, res );

      var childrenTablesHash = Object.keys( childTableData.layer.childrenTablesHash );

      // For each result, add them to resultObject
      async.eachSeries(
        res,

        function( item, cb ){

          currentObject.push( item );
          item._autoLoad = {};
          //resultObject[ childTable ].push( item );
 
          //console.log("RECORD ADDED:", item );
          ////console.log("ADDED TO (main):", resultObject );
          ////console.log("ADDED TO [childTable]:", resultObject[ childTable ]  );
          //console.log('SCANNING SUBTABLES FOR MORE:'  );
  
          async.eachSeries(
            Object.keys( childTableData.layer.childrenTablesHash ),
            function( recordSubTableKey, cb ){
  
              //console.log("FOUND: ", recordSubTableKey );
  
              item._autoLoad[ recordSubTableKey ] = [];

              var recordSubTableData = childTableData.layer.childrenTablesHash[ recordSubTableKey ]
  
              // Runs autoload again on the children, passing the array item._autoLoad[ recordSubTableKey ]
              // as the currentObject, and re-passing the v object (containing the end result)
              self._makeAutoLoad( childTableData.layer, item, recordSubTableKey, item._autoLoad[ recordSubTableKey ], v, function( err ){
                if( err ) return cb( err );
  
                cb( null );
              });
            },
            function( err ){
              if( err ) cb( err );
              //console.log("SUB SCAN FINISHED");
              cb( null );
            }
          );
        },
        function( err ){
          if( err ) return cb( err );

          //console.log("SCANNING OF RECORDS FINISHED." );
          cb( null, v.resultObject );
        }
      );

    });

  },

  _updateParentsAutoLoad: function( layer, record, cb ){

    var self = this;

    console.log("CALLED _updateParentsAutoLoad for ", layer.table ); 

    async.eachSeries(
      Object.keys( layer.parentTablesHash ),
      function( parentTableKey, cb ){

        console.log("PARENT KEY: ", parentTableKey );
        var parentTableData = layer.parentTablesHash[ parentTableKey ];

        var parentLayer = parentTableData.layer;
        var nestedParams = parentTableData.nestedParams;

        var andConditionsArray = [];
        Object.keys( nestedParams.join ).forEach( function( joinKey ){
          andConditionsArray.push( { field: nestedParams.join[ joinKey ], type: 'eq', value: record[ joinKey ] } );

          //parentFilter[ nestedParams.join[ joinKey ] ] = record[ joinKey ];
        });
        console.log("TELLING" , parentLayer.table, "TO UPDATE REFS FOR", layer.table, "FOR RECORD MATCHING", andConditionsArray );
      
        parentLayer.select( { conditions: { and: andConditionsArray } }, function( err, parentRecord, total ){
          if( err ) return cb( err );
          if( total > 1 ) return cb( new Error("PROBLEM: more than 1 parent in " + parentLayer.table + " for record " + record ) );

          // Update cache field in parent (need parent filter AND record)
          // Run self on parent layer/parent record

          console.log("PARENT RECORD:", parentRecord[ 0 ] );
          //console.log(' self._makeAutoLoad( ', parentLayer.table, parentRecord, layer.table, ')' );
          self._makeAutoLoad( parentLayer, parentRecord[ 0 ], layer.table, function( err, res ){
            if( err ) return cb( err );
            console.log("AND THE autoLoad data is:", res );


            /* TODO:  JUST UPDATE THE RECORD WITH THE NEW autoLoad STUFF AND IT'S DONE!!! */

            cb( null );

          });

        });
      },

      function( err ){
        if( err ) return cb( err );
        cb( null );
      }
    );

  },


  insert: function( record, options, cb ){

    var self = this;
    var recordToBeWritten = {};

    // Usual drill
    if( typeof( cb ) === 'undefined' ){
      cb = options;
      options = {}
    } else if( typeof( options ) !== 'object' || options === null ){
      return cb( new Error("The options parameter must be a non-null object") );
    }


    // Validate the record against the schema
    self.schema.validate( record, function( err, record, errors ){

      // If there is an error, end of story
      // If validation fails, call callback with self.SchemaError
      if( err ) return cb( err );
      if( errors.length ) return cb( new self.SchemaError( { errors: errors } ) );


      // Copy record over, only for existing fields
      for( var k in record ){
        if( typeof( self._fieldsHash[ k ] ) !== 'undefined' ) recordToBeWritten[ k ] = record[ k ];
      }

      // Every record in Mongo MUST have an _id field
      if( typeof( recordToBeWritten._id ) === 'undefined' ) recordToBeWritten._id  = ObjectId();

    /*
    // Sets the case-insensitive fields
    Object.keys( self._searchableHash ).forEach( function( fieldName ){
      if( self._searchableHash[ fieldName ] ){
        if( typeof( recordToBeWritten[ fieldName ] ) === 'string' ){
          recordToBeWritten[ '__uc__' + fieldName ] = recordToBeWritten[ fieldName ].toUpperCase();
        }
      }
    });
    */

      

      // Actually run the insert
      self.collection.insert( recordToBeWritten, function( err ){
        if( err ) {
          cb( err );
        } else {

          self._updateParentsAutoLoad( self, record, function( err, record ){

            if( ! options.returnRecord ){
              cb( null );
            } else {
              self.collection.findOne( { _id: recordToBeWritten._id }, self._projectionHash, function( err, doc ){
                if( err ){
                  cb( err );
                } else {

                  if( doc !== null && typeof( self._fieldsHash._id ) === 'undefined' ) delete doc._id;
                  cb( null, doc );
                }
              });
            }
          });

        };
      });
    });

  },

  'delete': function( filters, options, cb ){

    var self = this;

    // Usual drill
    if( typeof( cb ) === 'undefined' ){
      cb = options;
      options = {}
    } else if( typeof( options ) !== 'object' || options === null ){
      return cb( new Error("The options parameter must be a non-null object") );
    }

    // Run the query
    try { 
      var mongoParameters = this._makeMongoParameters( filters );
    } catch( e ){
      return cb( e );
    }

    // If options.multi is off, then use findAndModify which will accept sort
    if( !options.multi ){
      self.collection.findAndRemove( mongoParameters.querySelector, mongoParameters.sortHash, function( err, doc ) {
        if( err ) {
          cb( err );
        } else {

          if( doc ){
            cb( null, 1 );
          } else {
            cb( null, 0 );
          }
        }
    });

    // If options.multi is on, then "sorting" doesn't make sense, it will just use mongo's "remove"
    } else {
      self.collection.remove( mongoParameters.querySelector, { single: false }, cb );
    }

  },

  relocation: function( positionField, idProperty, id, moveBeforeId, conditionsHash, cb ){

    function moveElement(array, from, to) {
      if( to !== from ) array.splice( to, 0, array.splice(from, 1)[0]);
    }

    var self = this;

    // Sane value to filterHash
    if( typeof( conditionsHash ) === 'undefined' || conditionsHash === null ) conditionsHash = {};

    //console.log("REPOSITIONING BASING IT ON ", positionField, "IDPROPERTY: ", idProperty, "ID: ", id, "TO GO AFTER:", moveBeforeId );

    // Case #1: Change moveBeforeId
    var sortParams = { };
    sortParams[ positionField ] = 1;
    self.select( { sort: sortParams, conditions: conditionsHash }, { skipHardLimitOnQueries: true }, function( err, data ){
      if( err ) return cb( err );
      //console.log("DATA BEFORE: ", data );

      var from, to;
      data.forEach( function( a, i ){ if( a[ idProperty ].toString() == id.toString() ) from = i; } );
      //console.log("MOVE BEFORE ID: ", moveBeforeId, typeof( moveBeforeId )  );
      if( typeof( moveBeforeId ) === 'undefined' || moveBeforeId === null ){
        to = data.length;
        //console.log( "LENGTH OF DATA: " , data.length );
      } else {
        //console.log("MOVE BEFORE ID WAS PASSED, LOOKING FOR ITEM BY HAND...");
        data.forEach( function( a, i ){ if( a[ idProperty ].toString() == moveBeforeId.toString() ) to = i; } );
      }

      //console.log("from: ", from, ", to: ", to );

      if( typeof( from ) !== 'undefined' && typeof( to ) !== 'undefined' ){
        //console.log("SWAPPINGGGGGGGGGGGGGG...");

        if( to > from ) to --;
        moveElement( data, from, to);
      }

      //console.log("DATA AFTER: ", data );

      // Actually change the values on the DB so that they have the right order
      var item;
      for( var i = 0, l = data.length; i < l; i ++ ){
        item = data[ i ];

        updateTo = {};
        updateTo[ positionField ] = i + 100;
        //console.log("UPDATING...");
        self.update( { conditions: { and: [ { field: idProperty, type: 'eq', value: item[ idProperty ] } ] } }, updateTo, function(err,n){ /*console.log("ERR: " , err,n ); */} );
        //console.log( item.name, require('util').inspect( { conditions: { and: [ { field: idProperty, type: 'eq', value: item[ idProperty ] } ] } }, updateTo , function(){} ) );
        //console.log( updateTo );
      };

      cb( null );        
    });     

  },

  makeIndex: function( keys, options ){
    //console.log("MONGODB: Called makeIndex in collection ", this.table, ". Keys: ", keys );
    var opt = {};

    if( typeof( options ) === 'undefined' || options === null ) options = {};
    opt.background = !!options.background;
    opt.unique = !!options.unique;
    if( typeof( options.name ) === 'string' )  opt.name = options.name;

    this.collection.ensureIndex( keys, opt, function(){} );
  },

  dropAllIndexes: function( done ){
    //console.log("MONGODB: Called makeIndex in collection ", this.table, ". Keys: ", keys );
    var opt = {};

    this.collection.dropAllIndexes( done );
  },

});

// The default id maker
MongoMixin.makeId = function( object, cb ){
  if( object === null ){
    cb( null, ObjectId() );
  } else {
    cb( null, ObjectId( object ) );
  }
},

exports = module.exports = MongoMixin;


