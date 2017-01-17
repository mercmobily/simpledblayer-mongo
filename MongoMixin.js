/*jslint node: true, laxcomma:true */
"use strict";


/*
Copyright (C) 2015 Tony Mobily

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

var
  dummy

, declare = require( 'simpledeclare' )
, async = require( 'async' )
, debug = require( 'debug' )
, mongo = require( 'mongodb' )
, SimpleDbLayer = require('simpledblayer')
;

var consolelogOrig;

var consolelog = consolelogOrig = debug( 'simpledblayer:mongo');

function makeObjectId( id ){
  return ( id instanceof mongo.ObjectID) ? id : mongo.ObjectID( id );
}

var NEWAPI = 1;

//Differenciation BW TingoDb and MongoDB starts here

function _makeOperator( op ){
  return function( a, b ){
    var r = {};
    r[ a ] = { };
    r[ a ][ op ] =  b;
    return r;
  };
}

var MongoMixin = declare( Object, {

  _projectionHash: {},
  _fieldsHash: {},

  constructor: function(){

    var self = this;

    // Make up the _***Hash variables, which are used widely
    // within the module
    self._projectionHash = {};
    self._fieldsHash = {};

    //consolelog('\n\nINITIATING ', self );

    Object.keys( self.schema.structure ).forEach( function( field ) {
      var entry = self.schema.structure[ field ];
      self._fieldsHash[ field ] = true;
      if( ! entry.skipProjection ) self._projectionHash[ field ] = true;
    });

    // Create self.collection, used by every single query
    self.collection = self.db.collection( self.table );

    if( SimpleDbLayer.registry[ self.table ] ){
      throw new Error( "Only one layer instance can be created for table " + self.table );
    }
    SimpleDbLayer.registry[ self.table ] = self;

  },

  // The default id maker available as an object method
  makeId: function( object, cb ){
    MongoMixin.makeId( object, cb );
  },


  _isSearchableAsString: function( field ){
    return this._searchableHash[ field ] && this._searchableHash[ field ].type === 'string';
  },

  _addPrefix: function( field, fieldPrefix ){

    // mongoPath will be the full path
    return ( ( typeof( fieldPrefix ) === 'string' && fieldPrefix !== '' ) ? fieldPrefix + '.' : '' ) + field;

  },


  // If there are ".", then some children records are being referenced.
  // The actual way they are placed in the record is in _children; so,
  // add _children where needed.
  _addChildrenPrefixToPath: function( s ){

    if( s.match(/\./ ) ){

      var l = s.split( /\./ );
      for( var i = 0; i < l.length-1; i++ ){
        l[ i ] = '_children.' + l[ i ];
      }
      return l.join( '.' );

    } else {
      return s;
    }
  },

  _addUcPrefixToPath: function( s ){
    var a = s.split('.');
    a[ a.length - 1 ] = '__uc__' + a[ a.length - 1 ];
    return a.join('.');
  },

  _operators: {

    ne: _makeOperator( '$ne' ),

    lt: _makeOperator( '$lt' ),
    gt: _makeOperator( '$gt' ),

    lte: _makeOperator( '$lte' ),
    gte: _makeOperator( '$gte' ),

    eq: function( a, b ){
      var r = {};
      r[ a ] = b;
      return r;
    },

    near: function( a, b ){
      var r = {};

      r[ a ] = {
        $nearSphere: {
          $geometry: {
            type: "Point", coordinates: [ b[ 0 ], b[ 1 ] ]
          },
          $maxDistance: b[ 2]
        }

      }
      return r;
    },

    startsWith: function( a, b ){
      var r = {};
      r[ a ] = new RegExp('^' + b + '.*' );
      return r;
    },

    endsWith: function( a, b ){
      var r = {};
      r[ a ] = new RegExp('.*' + b + '$' );
      return r;
    },

    contains: function( a, b ){
      var r = {};
      r[ a ] =  new RegExp('^.*' + b + '.*$' );
      return r;
    }

  },


  // Make parameters for queries. It's the equivalent of what would be
  // an SQL creator for a SQL layer
  _makeMongoParameters: function( conditions, options, fieldPrefix, selectorWithoutBells ){
    return  {
      querySelector: this._makeMongoConditions( conditions || {}, fieldPrefix, selectorWithoutBells ),
      sortHash:  this._makeMongoSortHash( options && options.sort || {}, fieldPrefix )
    };
  },

  // Converts `conditions` to a workable mongo filters. Most of the complexity of this
  // function is due to the fact that it deals with the presence of `fieldPrefix` (in case
  // a child field is being modified, which will imply adding '_children.' to it) and
  // the presence of `selectorWithoutBells` (necessary for `$pull` operation in children)
  _makeMongoConditions: function( conditions, fieldPrefix, selectorWithoutBells, onlyLastPath ){

    //consolelog("FILTERS IN MONGO MIXIN: " );
    //consolelog( require('util').inspect( filters, {depth: 10 }) );

    var self = this;
    var a, aWithPrefix, aIsSearchableAsString, b;

    // If there is no condition, return an empty filter
    if( ! conditions.type ) return {};

    // Scan filters recursively, making up the mongo query
    if( conditions.type == 'and' || conditions.type == 'or' ){

      // For 'and', it will return { $and: [ ... ] }
      var mongoName = '$' + conditions.type;

      // The content of the $and key will be the result of makeMongo
      // wr stands for "working return". Normally it's the same as "r"; however,
      // $elemMatch might mean that wr is r.$elemMatch
      var r = {};
      var wr = r;

      var pr;

      // onlyLastPath is set from before.
      if( onlyLastPath ){
        selectorWithoutBells = true;

      // onlyLastPath is NOT set from before, but there is an elemMatch
      } else if( pr = conditions.elemMatch  ){
        selectorWithoutBells = true;

        r[ '_children.' + pr ] = { $elemMatch: {} } ;
        wr = r[ '_children.' + pr ].$elemMatch;
        onlyLastPath = pr;
      }

      wr[ mongoName ] = conditions.args.map( function( item ){
        return self._makeMongoConditions( item, fieldPrefix, selectorWithoutBells, onlyLastPath );
      });

      return r;

    } else {

      // Otherwise, run the operator encoutered
      // (But, remember to fixup the field name (paths, etc.) and possibly the checked value (uppercase)
      var operator = this._operators[ conditions.type ];
      if( ! operator ) throw( new Error( "Could not find operator: " + conditions.type ) );

      // Save this for later
      a = conditions.args[ 0 ];
      b = conditions.args[ 1 ];

      // Safeguard just in case
      if( a.indexOf( '.') != -1 && onlyLastPath ){
        if( onlyLastPath != a.split('.')[0] ) throw new Error("When using elemMatch, sub-paths must all match" );
      }

      // Making up aWithPrefix, useful to check if it's searchable, if
      // b should be converted to upperCase(), etc.

      // Create aWithPrefix, which is simply `a` with the prefix prepended
      // to it
      aWithPrefix = this._addPrefix( a, fieldPrefix );

      // Check that aWithPrefix is indeed searchable
      if( !self._searchableHash[ aWithPrefix ] ){
        throw( new Error("Field " + aWithPrefix + " is not searchable" ) );
      }

      // Cast the field to the right type in the schema
      // TODO: Really, seriously, implement a nice way in simpleSchema to
      // cast a single field without this level of ugliness

      //console.log("Type of b before the cure:" , a, conditions, typeof b );
      var definition = self._searchableHash[ aWithPrefix ];
      var definitionSchema = self._searchableHashSchema[ aWithPrefix ];
      var type = definition.type;
      var castFunction = type && definitionSchema[ type + 'TypeCast'];
      var failedCasts = {};
      var errors = [];
      //console.log( type, castFunction, definitionSchema );
      if( type &&  castFunction ){
        b = castFunction.call( definitionSchema, definition, b, aWithPrefix, {}, failedCasts );
      }
      Object.keys( failedCasts ).forEach( function( fieldName ){
        errors.push( { field: aWithPrefix, message: "Error during casting" } );
      });

      // There was an error while casting: through a SchemaError
      if( errors.length ){
        throw new self.SchemaError( { errors: errors } );
      }

      // Create aIsSearchableAsString. Saving the result as I will need the result
      // if this check later on, to determine whether to add __uc__ to `a`
      aIsSearchableAsString = this._isSearchableAsString( aWithPrefix );

      // `upperCase()` `b` if a is of type 'string', since it will be compared to
      // the __uc__ equivalent field
      b = aIsSearchableAsString ? b.toUpperCase() : b;

      // Unless we want a selector without path (only used when `$pull`ing an array in `deleteMany`),
      // `a` needs to become `aWithPrefix`.

      a = selectorWithoutBells ? a : aWithPrefix;
      if( aIsSearchableAsString ) a = self._addUcPrefixToPath( a );

      if( onlyLastPath ){
        a = a.substr( a.lastIndexOf('.') + 1);
      } else {
        // Add __uc__ (if field is 'string') to path

        // Add _children to `a` (unless a selector without bells was required)
        a = selectorWithoutBells ? a : self._addChildrenPrefixToPath( a );
      }

      // Call the operator on the two values
      return operator.call( this, a, b, fieldPrefix, selectorWithoutBells );
    }

  },


  _makeMongoSortHash: function( sort, fieldPrefix ){

    var self = this;
    var sortHash = {};

    //consolelog( "_sortableHash is:", self._sortableHash );
    for( var field  in sort ){
      if( ! sort.hasOwnProperty( field) ) continue;
      var sortDirection = sort[ field ];

      var searchableHashEntry = ( fieldPrefix ? fieldPrefix + '.' : '' ) + field;
      if( self._searchableHash[ searchableHashEntry ] || field === self.positionBaseField ){

        // Add prefix to the field
        field = this._addPrefix( field, fieldPrefix );

        // Check that it's searchable -- if not, it's not sortble either
        if( !self._searchableHash[ field ] ){
          throw( new Error("Field " + field + " is not searchable, and therefore not sortable" ) );
        }

        if( self._isSearchableAsString( field ) ){
          field = this._addUcPrefixToPath( field );
        }
        field = this._addChildrenPrefixToPath( field );

        sortHash[ field ] = sortDirection;
      }
    }
    consolelog( "FINAL SORTHASH", self.table );
    consolelog( require('util').inspect( sortHash, { depth: 10 } ) );

    return sortHash;
  },


  cleanRecord: function( obj, skip, cb ){

    var self = this;

    consolelog("*******CLEANRECORD CALLED! OBJECT, SKIP", obj, skip );

    // If skip, then don't do anything
    if( skip ) return cb( null, obj );

    self._completeRecord( obj, function( err, obj ){
      if( err ) return cb( err );

      // Temporarily add UcFields and _clean, since about to do update
      // NOTE: it's IMPORTANT to pass all fields to the update, because
      // the change in the schema could just be that a string field has
      // become searchable.
      self._addUcFields( obj );
      obj._clean = true;

      // Update record. Note that the record will be the exact same as it was, except:
      // - _clean is true
      // - UC fields are added (as they should be)
      // - _children is populated by _completeRecord
      var updateQuery = {};
      updateQuery[ self.idProperty ] = obj[ self.idProperty ];
      self.collection.update( updateQuery, { $set: obj }, { multi: false }, function( err, total ){
        if( err ) return cb( err );
        if( !total ) return cb( new Error("Record to be cleared not found") );

        // Delete UcFields and _clean since returning the object
        // Note that there is no point in cleaning up the children, since
        // _completeRecord takes care of it.
        delete obj._clean;
        self._deleteUcFields( obj );

        cb( null, obj );
      });
    });
  },


  dirtyRecord: function( obj, cb ){
    var self = this;

    // Update record so that it's marked as "dirty"
    var updateQuery = {};
    updateQuery[ self.idProperty ] = obj[ self.idProperty ];
    consolelog("DOING: ", updateQuery );
    self.collection.update( updateQuery, { $set: { _clean: false } }, { multi: false }, cb );
  },

  dirtyAll: function( cb ){
    var self = this;

    // Update ALL records so that they are marked as "dirty"
    self.collection.update( {}, { $set: { _clean: false } }, { multi: true }, cb );
  },

  dirtyAllParents: function( cb ){
    var self = this;

    async.each(
      self.parentTablesArray,
      function( layer, cb ){

        self.parentTablesArray.forEach( function( layer ){
          // Update ALL records so that they are marked as "dirty"
          layer.layer.collection.update( {}, { $set: { _clean: false } }, { multi: true }, cb );
       });
      },
      cb
    );
  },


  select: function( conditions, options, cb ){

    // This is for 'historical' reasons -- and because it's easier to find `noChildren`
    // in the code
    var noChildren;
    if( typeof options.children != 'undefined' && options.children == false ){
      noChildren = true;
    }


    //console.log("RAN SELECT WITH", this.table, conditions );
    //if( typeof options == 'object') console.log("And options:", options );

    var self = this;
    var saneRanges;

    // Usual drill
    if( typeof( cb ) === 'undefined' ){
      cb = options;
      options = {};
    } else if( typeof( options ) !== 'object' || options === null ){
      return cb( new Error("The options parameter must be a non-null object") );
    }


    // Normalise parameter to make things compatible with old API
    var warning;
    if( conditions.sort ){ warning = true; options.sort = conditions.sort; }
    if( conditions.ranges){ warning = true; options.ranges = conditions.ranges; }
    if( conditions.conditions ){ warning = true; conditions = conditions.conditions; }
    if( warning ){
      var caller_line = (new Error).stack.split("\n")[2]
      console.warn("WARNING: Using the old-style API for select, parameters were normalised:", caller_line );
    }

    // Make up parameters from the passed filters
    var mongoParameters;
    try {
      mongoParameters = this._makeMongoParameters( conditions, options );
      //consolelog("MONGO PARAMETERS:", require('util').inspect( mongoParameters, {depth: 10 } ) );
    } catch( e ){
      return cb( e );
    }


    if( options.blockEmptyFilter && Object.keys( mongoParameters.querySelector ).length === 0 ){
      return cb( new Error("Cannot run on empty query") );
    }

    consolelog("MONGO PARAMETERS:", require('util').inspect( mongoParameters, { depth: 10 } ) );

    // If sortHash is empty, AND there is a self.positionField, then sort
    // by the element's position
    consolelog("TABLE:", self.table );
    consolelog("OPTIONS", options );
    consolelog("CONDITIONS", require('util').inspect( conditions, { depth: 10 } ) );

    consolelog("SORT HASH", mongoParameters.sortHash );
    consolelog( Object.keys( mongoParameters.sortHash ).length );
    consolelog( self.positionField );

    if( Object.keys( mongoParameters.sortHash ).length === 0 && self.positionField ){
      mongoParameters.sortHash[ self.positionField ] = 1;
    }

    //consolelog("CHECK THIS:");
    //consolelog( require('util').inspect( mongoParameters, { depth: 10 } ) );

    // The projectionHash hash will always include:
    //  * _clean (which will tell the driver if a record is actually clean, which
    //     means all of its _children are up to date) and
    // * _children  (which is the list of children, only if _children is true)
    var projectionHash = {};
    for( var k in self._projectionHash ){
      if( ! self._projectionHash.hasOwnProperty( k ) ) continue;
      projectionHash[ k ] = self._projectionHash[ k ];
    }

    if( ! noChildren ) projectionHash._children = true;
    consolelog("SO PROJECTIONHASH IS:", projectionHash );


    consolelog("PH: ", options, require('util').inspect( mongoParameters.querySelector, { depth: 10 } ), projectionHash );
    consolelog("TABLE: ", self.table );

    // Actually run the query
    consolelog("PROJECTION HASH:", this.table, projectionHash );
    var cursor = self.collection.find( mongoParameters.querySelector).project( projectionHash );
    consolelog("FIND IN SELECT: ",  require('util').inspect( mongoParameters.querySelector, { depth: 10 } ) );

    // Sanitise ranges. If it's a cursor query, or if the option skipHardLimitOnQueries is on,
    // then will pass true (that is, the skipHardLimitOnQueries parameter will be true )
    saneRanges = self.sanitizeRanges( options.ranges, options.useCursor || options.skipHardLimitOnQueries );


    consolelog("SORTING THIS VIA MONGODB AS:", mongoParameters.sortHash );
    cursor.sort( mongoParameters.sortHash );

    // Skipping/limiting according to ranges/limits
    if( saneRanges.skip )  cursor.skip( saneRanges.skip );
    if( saneRanges.limit ) cursor.limit( saneRanges.limit );

    // Sort the query

    if( options.useCursor ){

      cursor.count( false, function( err, grandTotal ){
        if( err ) return cb( err )

        cursor.count( true, function( err, total ){
          if( err ) return cb( err );

          cb( null, {

            each: function( iterator, endCallback ){

              var i;
              var self = this;
              async.doWhilst(

                function( callback ){

                  self.next( function( err, element ){
                    if( err ) return callback( err );

                    i = element;

                    // If the element is null, nothing to do. This will also
                    // be the last iteration of this async.doWhilst cycle
                    if( element === null ) return callback( null );

                    iterator( element, function( err, breakFlag ){
                      if( err ) return callback( err );

                      // If breakFlag, force quitting the cycle (neatly)
                      if( breakFlag ) i = null;

                      callback( null );

                    });
                  });
                },
                function(){ return i !== null; },

                function( err ) {
                  if( err ) return endCallback( err );

                  endCallback( null );
                }
              );

            },

            next: function( done ){

              cursor.nextObject( function( err, obj ) {
                if( err ) return done( err );

                // Returned null: nothing to see here
                if( obj === null ) return done( null, null );


                // Mongo will return _id: if it's not in the schema, zap it
                // (Will save it first, in case it will be needed for deletion)
                var _id = obj._id;
                if( typeof( self._fieldsHash._id ) === 'undefined' )  delete obj._id;

                // We will need this later if option.children was true, as
                // schema.validate() will wipe it
                if( ! noChildren ) var _children = obj._children;
                var clean = obj._clean;

                self.schema.validate( obj, { ignoreFieldsWithAttributes: [ 'doNotSave' ], deserialize: true, ignoreFields: [ '_children', '_clean' ] }, function( err, obj, errors ){

                  // If there is an error, end of story
                  // If validation fails, call callback with self.SchemaError
                  if( err ) return cb( err );
                  if( self.strictSchemaOnFetch && errors.length ){
                    var e = new self.SchemaError( "Schema is strict and loaded data didn't match" );
                    e.errors = errors;
                    return cb( e );
                  }

                  // Re-add children, since it may be required later and was zapped by
                  // schema.validate()
                  if( ! noChildren ) obj._children = _children;

                  // If the object isn't clean, then it will trigger the _completeRecord
                  // call which will effectively complete the record with the right _children
                  // Note that after this call obj._children may or may not get
                  // overwritten (depends wheter _cleanRecord gets skipped).
                  var skip = noChildren || clean;
                  self.cleanRecord( obj, skip, function( err, obj ){
                    if( err ) return done( err );

                    // Note: at this point, _children might be either the old existing one,
                    // or a new copy created by _cleanRecord
                    // At this point, sort out _children (which might get deleted
                    // altogether or might have extra _uc__ fields) AND
                    // get rid of obj._clean which isn't meant to be returned to the user
                    if( ! noChildren )
                      self._deleteUcFieldsAndCleanfromChildren( _children );
                    delete obj._clean;

                    // If options.delete is on, then remove the record before returning it
                    if( options.delete ){
                      self.collection.remove( { _id: _id }, function( err, r ){
                        if( err ) return done( err );

                        return done( null, obj );
                      });
                    } else {
                      done( null, obj );
                    }
                  });
                });
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


        });


      });

    } else {


      cursor.toArray( function( err, queryDocs ){
        if( err ) return cb( err );

        cursor.count( false, function( err, grandTotal ){
          if( err ) return cb( err );

          consolelog("Err:", err );

          consolelog("QUERYDOCS: ", queryDocs );

          var _id;

          var index = -1 ;
          async.eachSeries(

            queryDocs,

            function( doc, callback ){

              // Index starts from 0
              index ++;

              // If options.delete is on, we will need doc._id to delete this item
              if( options.delete ) _id = doc._id;

              // Mongo will return _id: if it's not in the schema, zap it
              if( typeof( self._fieldsHash._id ) === 'undefined' ) delete doc._id;

              // We will need this later if option.children was true, as
              // schema.validate() will wipe it
              if( ! noChildren ) var _children = doc._children;
              var clean = doc._clean;

              self.schema.validate( doc, { ignoreFieldsWithAttributes: [ 'doNotSave' ], deserialize: true, ignoreFields: [ '_children', '_clean' ] }, function( err, validatedDoc, errors ){

                if( err ) return callback( err );

                if( self.strictSchemaOnFetch && errors.length ) {
                  var e =  new self.SchemaError( "Schema is strict and loaded data didn't match");
                  e.errors = errors;
                  return cb( e );
                }

                // Re-add children, since it may be required later and was zapped by
                // schema.validate()
                if( !noChildren ) validatedDoc._children = _children;

                // If the object isn't clean, then it will trigger the _completeRecord
                // call which will effectively complete the record with the right _children
                // and add any UC fields needed
                var skip = clean || noChildren;
                self.cleanRecord( validatedDoc, skip, function( err, validatedDoc ){
                  if( err ) return callback( err );

                  // Note: at this point, _children might be either the old existing one,
                  // or a new copy created by _cleanRecord
                  // Sort out _children AND
                  // get rid of obj._clean which isn't meant to be returned to the user
                  if( !noChildren )
                    self._deleteUcFieldsAndCleanfromChildren( _children );
                  delete validatedDoc._clean;

                  queryDocs[ index ] = validatedDoc;

                  // If options.delete is false, then this functon is done
                  if( ! options.delete ) return callback( null );

                  // If options.delete is true, then attempt to delete the record first
                  self.collection.remove( { _id: _id }, function( err ){
                    if( err ){
                      delete queryDocs[ index ];
                      err.errorInDelete = true; // The error happened during deletion
                      return callback( err );
                    }
                    callback( null );
                  });

                });
              });
            },

            // Note that an error in deletion is NOT going to cause this to fail.
            // TODO: maybe emit a signal, or do SOMETHING to notify the error in case of errorInDelete
            function( err ){
              if( err && ! err.errorInDelete ) return cb( err );

              // That's all!
              cb( null, queryDocs, queryDocs.length, grandTotal );
            }
          );
        });
      });
    }
  },


  update: function( conditions, updateObject, options, cb ){

    var self = this;

    var unsetObject = {};

    var rnd = Math.floor(Math.random()*100 );
    consolelog( "\n");
    consolelog( rnd, "ENTRY: update for ", self.table, ' => ', updateObject, "options:", options );

    // Usual drill
    if( typeof( cb ) === 'undefined' ){
      cb = options;
      options = {};
    } else if( typeof( options ) !== 'object' || options === null ){
      return cb( new Error("The options parameter must be a non-null object") );
    }

    // You cannot do a multiple update when options.deleteUnsetFields is set, as _every_
    // required field needs to be specified -- and the idProperty is obviously required
    // deleteUnsetFields is there so that a "document.save()"-style called can be performed
    // easily.
    if( options.multi && options.deleteUnsetFields ){
      return cb( new Error("The options 'multi' and 'deleteUnsetFields' are mutually exclusive -- one or the other") );
    }

    // Validate the record against the schema

    // If deleteUnsetFields is set, then the validation will apply to _every_ field in the schema.
    // Otherwise, just to the passed fields is fine
    var onlyObjectValues = !options.deleteUnsetFields;

    self.emitCollect( 'simpledblayer-pre-update', { table: self, conditions: conditions, updateObject: updateObject, options: options }, function( err ){
      if( err ) return cb( err );

      // Assigning to `null` means zapping them.
      // TODO: Either make this mandatory in the API, or implement a better way
      // of unsetting fields
      Object.keys( self._fieldsHash ).forEach( function( i ){

        if( updateObject[ i ] === null ) {

          unsetObject[ i ] = 1;
          delete updateObject[ i ];
          if( self._isSearchableAsString( i ) ){
            unsetObject[ '__uc__' + i ] = 1;
          }
        }
      });

      // Validate what was passed...
      self.schema.validate( updateObject, { onlyObjectValues: onlyObjectValues, skipValidation: options.skipValidation }, function( err, updateObject, errors ){

        // If there is an error, end of story
        // If validation fails, call callback with self.SchemaError
        if( err ) return cb( err );

       if( errors.length ){
          var SchemaError = new self.SchemaError( { errors: errors } );
          SchemaError.errors = errors;
          return cb( SchemaError );
        }

        // The _id field cannot be updated. If it's there,
        // simply delete it
        delete updateObject._id;

        // Add __uc__ fields to the updateObject (they are the uppercase fields
        // used for filtering of string fields)
        self._addUcFields( updateObject );

        // If `options.deleteUnsetFields`, Unset any value that is not actually set but IS in the schema,
        // so that partial PUTs will "overwrite" whole objects rather than
        // just overwriting fields that are _actually_ present in `body`
        // NOTE: fields marked as "protected" in the schema are spared, as they are... well, protected!
        if( options.deleteUnsetFields ){
          Object.keys( self._fieldsHash ).forEach( function( i ){
            if( !self.schema.structure[ i ].protected && typeof( updateObject[ i ] ) === 'undefined' && i !== '_id' && i !== self.positionField && i != '_clean' ){
              unsetObject[ i ] = 1;

              // Get rid of __uc__ objects if the equivalent field was taken out
              if( self._isSearchableAsString( i ) ){
                unsetObject[ '__uc__' + i ] = 1;
              }
            }
          });
        }

        // Make up parameters from the passed filters
        var mongoParameters;
        try {
          mongoParameters = self._makeMongoParameters( conditions );
        } catch( e ){
          return cb( e );
        }

        if( !self.allowEmptyQueryOnUpdate && !options.allowEmptyQuery && Object.keys( mongoParameters.querySelector ).length === 0 ){
          return cb( new Error("Cannot run update on empty query") );
        }

        consolelog( rnd, "About to update. At this point, updateObject is:", updateObject );
        consolelog( rnd, "Selector:", mongoParameters.querySelector );
        consolelog( rnd, "Unset object::", unsetObject );


        self._makeUpdateAndUnsetObjectWithLookups( updateObject, unsetObject, function( err, updateObjectWithLookups, unsetObjectWithLookups  ){
          if( err ) return cb( err );

          consolelog( rnd, "Update object with lookups:", updateObjectWithLookups );
          consolelog( rnd, "Unset object with lookups:", unsetObjectWithLookups );

          // If options.multi is off, then use findAndModify which will return the doc
          if( !options.multi ){

            var u = {};
            if( Object.keys( updateObjectWithLookups ).length )  u.$set = updateObjectWithLookups;
            if( Object.keys( unsetObjectWithLookups ).length ) u.$unset = unsetObjectWithLookups;
            self.collection.findAndModify( mongoParameters.querySelector, {}, u, { new: true }, function( err, doc, n ){
              if( err ) return cb( err );

              // Patched for mongo driver 2.0
              // TODO: findAndModify is deprecated, update to a current function AND probably delete this
              if( NEWAPI ) doc = doc.value;

              if( doc ){

                // Change parents so that the one record is updated
                self._updateParentsRecords( { op: 'updateOne', id: doc[ self.idProperty ], updateObject: updateObject, unsetObject: unsetObject }, function( err ){
                  if( err ) return cb( err );

                  // Getting the full record so that I can emit
                  self.selectById( doc[ self.idProperty ], function( err, fullRecord ){
                    if( err ) return cb( err );

                    self.emitCollect( 'simpledblayer-update-one', { table: self, record: fullRecord, conditions: conditions, updateObject: updateObject, options: options }, function( err ){
                      if( err ) return cb( err );

                      cb( null, 1, fullRecord );
                    });
                  })
                });
              } else {
                cb( null, 0, null );
              }
            });

          // If options.multi is on, no document will be returned: it will just use mongo's "update"
          } else {

            // Run the query

            var u = { $set: updateObjectWithLookups };
            if( Object.keys( unsetObjectWithLookups ).length ) u.$unset = unsetObjectWithLookups;
            self.collection.update( mongoParameters.querySelector, u, { multi: true }, function( err, r ){
              if( err ) return cb( err );

              if( NEWAPI ) var total = r.result.n;
              else total = r;

              // MONGO: Change parents
              self._updateParentsRecords( { op: 'updateMany', conditions: conditions, updateObject: updateObject, unsetObject: unsetObject }, function( err ){
                if( err ) return cb( err );

                self.emitCollect( 'simpledblayer-update-many', { table: self, conditions: conditions, updateObject: updateObject, options: options}, function( err ){
                  if( err ) return cb( err );

                  cb( null, total );
                });
              });
            });
          }

        });
      });

    });

  },


  insert: function( record, options, cb ){

    var self = this;

    var rnd = Math.floor(Math.random()*100 );
    consolelog( "\n");
    consolelog( rnd, "ENTRY: insert for ", self.table, ' => ', record );

    // Usual drill
    if( typeof( cb ) === 'undefined' ){
      cb = options;
      options = {};
    } else if( typeof( options ) !== 'object' || options === null ){
      return cb( new Error("The options parameter must be a non-null object") );
    }

    // TODO: maybe this should happen after validation. BUT careful, because
    // validation is optional... so maybe it shouldn't. And maybe makeId
    // shouldn't really care about the record itself.
    if( typeof record[ self.idProperty ] === 'undefined' ){
      self.schema.makeId( record, function( err, id ){
        record[ self.idProperty ] = id;
        restOfFunction();
      });
    } else {
      restOfFunction();
    }

    function restOfFunction(){

      self.emitCollect( 'simpledblayer-pre-insert', { table: self, record: record, options: options }, function( err ){
        if( err ) return cb( err );

        // Validate the record against the schema
        self.schema.validate( record, { skipValidation: options.skipValidation }, function( err, record, errors ){

          // If there is an error, end of story
          // If validation fails, call callback with self.SchemaError
          if( err ) return cb( err );

          if( errors.length ){
            var SchemaError = new self.SchemaError( { errors: errors } );
            SchemaError.errors = errors;
            return cb( SchemaError );
          }

          consolelog( rnd, "record after validation:", record );

          // Add __uc__ fields to the record
          self._addUcFields( record );

          consolelog( rnd, "record with __uc__ fields:", record );

          self._completeRecord( record, function( err, recordWithLookups ){
            if( err ) return cb( err );

            consolelog( rnd, "recordWithLookups is:", recordWithLookups);

            // Every record in Mongo MUST have an _id field. Note that I do this here
            // so that record doesn't include an _id field when self._makeRecordWithLookups
            // is called (which would imply that $pushed, non-main children elements would also
            // have _id
            if( typeof( recordWithLookups._id ) === 'undefined' ) recordWithLookups._id  = makeObjectId();

            recordWithLookups._clean = true;

            consolelog( rnd, "record with _id added:", record );
            consolelog( rnd, "ADDING:", recordWithLookups );

            // Actually run the insert
            self.collection.insert( recordWithLookups, function( err ){
              if( err ) return cb( err );

              // Call self.reposition if self.positionField is set. This only happens
              // automatically on insers, obviously.
              // This will get called shortly, and will call cb() straight away if no
              // repositioning is needed
              var repositionIfNeeded = function( cb ){
                if( ! self.positionField ) return cb( null );

                var where, beforeId;
                if( ! options.position ){
                  where = 'end';
                } else {
                  where = options.position.where;
                  beforeId = options.position.beforeId;
                }
                self.reposition( recordWithLookups, where, beforeId, cb );

              };
              repositionIfNeeded( function( err ){
                if( err ) return cb( err );

                self._updateParentsRecords( { op: 'insert', record: record }, function( err ){
                  if( err ) return cb( err );

                  var opt = {};
                  if( typeof opt.children != 'undefiend' ) opt.children = options.children;
                  // Re-fetch the record using the API
                  self.selectById( record[ self.idProperty ], opt, function( err, fetchedRecord ){
                    if( err ) return cb( err );

                    // Emit the insert event
                    self.emitCollect( 'simpledblayer-insert', { table: self, record: fetchedRecord, options: options }, function( err ){
                      if( err ) return cb( err );

                      return cb( null, fetchedRecord );
                    });
                  })
                });
              });
            });
          });
        });
      });
    };
  },


  'delete': function( conditions, options, cb ){

    var self = this;

    var rnd = Math.floor(Math.random()*100 );
    consolelog( "\n");
    consolelog( rnd, "ENTRY: delete for ", self.table, ' => ', require('util').inspect( conditions, { depth: 10 }  ) );

    // Usual drill
    if( typeof( cb ) === 'undefined' ){
      cb = options;
      options = {};
    } else if( typeof( options ) !== 'object' || options === null ){
      return cb( new Error("The options parameter must be a non-null object") );
    }


    self.emitCollect( 'simpledblayer-pre-delete', { table: self, conditions: conditions, options: options }, function( err ){
      if( err ) return cb( err );


      // If options.multi is off, then use findAndModify which will give us the ID of the modify one (which
      // will be passed to _updateParentRecords)
      if( !options.multi ){

        // Fetch the record that is about to be deleted
        self.select( conditions, { ranges: { limit: 1 }, blockEmptyFilter: !options.allowEmptyQuery && !self.allowEmptyQueryOnDelete }, function( err, fetchedRecords ){
          if( err ) return cb( err );

          // Nothing is there: call callback with 0
          if( fetchedRecords.length === 0 ) return cb( null, 0 );

          // The first item is the one that will get deleted
          var fetchedRecord = fetchedRecords[ 0 ];

          var deleteSelector = {}
          deleteSelector[ self.idProperty ] = fetchedRecord[ self.idProperty ];

          // Fetch the record that is about to be deleted
          self.collection.remove( deleteSelector, { single: true }, function( err, n ){
            if( err ) return cb( err );

            self._updateParentsRecords( { op: 'deleteOne', id: fetchedRecord[ self.idProperty ] }, function( err ){
              if( err ) return cb( err );

              self.emitCollect( 'simpledblayer-delete-one', { table: self, record: fetchedRecord, conditions: conditions, options: options }, function( err ){
                if( err ) return cb( err );

                // Call callback with 1, the number of deleted records
                // TODO: Update API so that it's clear that fetched record is returned
                cb( null, 1, fetchedRecord );
              });
            });
          });
        })

      // If options.multi is on, then simply use `remove`, as _updateParentsRecords will simply get
      // the filters as parameter
      } else {

        // Make up the query for remove
        var mongoParameters;
        try {
          mongoParameters = self._makeMongoParameters( conditions );
        } catch( e ){
          return cb( e );
        }

        if( !options.allowEmptyQuery && !self.allowEmptyQueryOnDelete && Object.keys( mongoParameters.querySelector ).length === 0 ){
          return cb( new Error("Cannot run delete on empty query") );
        }


        self.collection.remove( mongoParameters.querySelector, { single: false }, function( err, r ){
          if( err ) return cb( err );

          if( NEWAPI ) var total = r.result.n;
          else var total = r;

          self.emitCollect( 'simpledblayer-delete-many', { conditions: conditions, options: options }, function( err ){
            if( err ) return cb( err );

            self._updateParentsRecords( { op: 'deleteMany', conditions: conditions }, function( err ){
              if( err ) return cb( err );

              cb( null, total );
            });
          });
        });
      }
    })

  },

  reposition: function( record, where, beforeId, cb ){

    // No position field: nothing to do
    if( ! this.positionField ){
       consolelog("No positionField for this table, skipping repositioning altogether: ", this.table );
       return cb( null );
    }

    consolelog("Reposition called on ", record, " to be moved here:", where, "With beforeId being", beforeId );

    function moveElement(array, from, to) {
      if( to !== from ) array.splice( to, 0, array.splice(from, 1)[0]);
    }

    var self = this;

    var one = false;
    var positionField = self.positionField;
    var idProperty = self.idProperty;
    var conditionsHash = {};
    var id = record[ idProperty ];

    // Make up conditionsHash based on the positionBase array
    //conditionsHash = { type: 'and', args: [] };
    //for( var i = 0, l = self.positionBase.length; i < l; i ++ ){
    //  var positionBaseField = self.positionBase[ i ];
    //  conditionsHash.args.push( { type: 'eq', args: [ positionBaseField, record[ positionBaseField ] ] } );
    //  one = true;
    //}
    conditionsHash = {};
    for( var i = 0, l = self.positionBase.length; i < l; i ++ ){
      var positionBaseField = self.positionBase[ i ];
      conditionsHash[ positionBaseField ] = record[ positionBaseField ];
    }

    consolelog("Repositioning basing it on", positionField, "conditionsHash:", conditionsHash, "positionBase: ", self.positionBase, "idProperty: ", idProperty, "id: ", id );

    // Run the select, ordered by the positionField and satisfying the positionBase
    var sortParams = { };
    sortParams[ positionField ] = 1;
    self.selectByHash( conditionsHash, {  sort: sortParams, skipHardLimitOnQueries: true }, function( err, data ){
      if( err ) return cb( err );

      consolelog("Data before: ", data );

      // Working out `from` as a potitional number in the array
      var from, to;
      data.forEach( function( a, i ){ if( a[ idProperty ].toString() == id.toString() ) from = i; } );

      // Set 'from' and 'to' depending on parameters
      switch( where ){
        case 'start':  to = 0; break;
        case 'end': to = data.length; break;
        case 'before':
          data.forEach( function( a, i ){ if( a[ idProperty ].toString() == beforeId.toString() ) to = i; } );
        break;
      }

      consolelog("FROM AND TO AFTER READING PARAMETERS: from: ", from, ", to: ", to );

      // Actually move the elements
      if( typeof( from ) !== 'undefined' && typeof( to ) !== 'undefined' ){
        consolelog("Swapping!!!");

        if( to > from ) to --;
        moveElement( data, from, to);

        consolelog("Data after: ", data );

        // Actually change the values on the DB so that they have the right order
        var updateCalls = [];
        data.forEach( function( item, i ){

          consolelog("Item: ", item, i );
          var updateTo = {};
          updateTo[ positionField ] = i + 100;

          updateCalls.push( function( cb ){
            var mongoSelector = {};
            mongoSelector[ idProperty ] = item[ idProperty ];
            consolelog("Updating...", mongoSelector, { $set: updateTo } );
            self.collection.update( mongoSelector, { $set: updateTo }, cb );
          });

        });

        // Runs the updates in series, calling the final callback at the end
        async.series( updateCalls, function( err ){
          if( err ) return cb( err );

          consolelog("******************************REPOSITIONED!" )

          consolelog( "MULTIPLE:", self.table, self.parentTablesArray );

          // TODO POSITIONING: update all parents which a matching entry in _children Setting
          // _clean to false. This means that its children will be regenerated on new fetch





          cb( null );

        } );




      } else {

        // Something went wrong, no changes will be made
        cb( null );
      }

    });


  },

  dropIndex: function( name, cb ){

    this.collection.dropIndex( name );

    // The mongo call is synchronous, call callback manually
    cb( null );
  },

  dropAllIndexes: function( done ){
    this.collection.dropAllIndexes( done );
  },

  makeIndex: function( keys, name, options, cb ){
    //consolelog("MONGODB: Called makeIndex in collection ", this.table, ". Keys: ", keys );
    var opt = {};

    // This is important or the call will stall
    if( Object.keys( keys).length === 0 ) return cb( null );

    consolelog("INDEXING", this.table, "index name:", name, "with keys:", keys, ' options:', options );

    if( typeof( options ) === 'undefined' || options === null ) options = {};
    opt.background = !!options.background;
    //opt.background = false;
    opt.unique = !!options.unique;
    if( typeof( name ) === 'string' )  opt.name = name;

    consolelog("ONE", keys, opt);
    this.collection.createIndex( keys, opt, function( err ){
      if( err ) return cb( err );

      consolelog("TWO", keys, opt);
      cb( null );
    });
  },

  // Make all indexes based on the schema
  // Options can have:
  //   `{ background: true }`, which will make sure makeIndex is called with { background: true }
  //
  // NOTE TO Db Layer DEVELOPERS USING THIS AS A TEMPLATE:
  // I used the _indexGroups variable as mongoDB requires very specific indexing (each record "contains"
  // its children directly, and the children's fields also need to be indexed within the parent).
  // In normal circumstances, just scan the layer's schema for anything `searchable`.

  _addAllPrefixes: function( s, prefix ){

    consolelog("S IS:", s );
    //if( prefix && s.indexOf('.') === -1 )s = prefix + '.' + s;
    if( prefix ) s = prefix + '.' + s;

    consolelog("S NOW IS: ", s );

    consolelog("s is", s );
    consolelog("isSearchableAsString is:");
    consolelog( require('util').inspect( this._searchableHash ) );

    // Add the __uc prefix (if necessary)
    if( this._isSearchableAsString( s ) ) {
      s = this._addUcPrefixToPath( s );
    }
    // Turn it into a proper field path (it will only affect fields with "." in them)
    s = this._addChildrenPrefixToPath( s );

    return s;
  },

  _makeSignature: function( o ){
    var s = '';
    Object.keys( o ).sort().forEach( function( k ) { s += k; } );
    return s;
  },

  generateSchemaIndexes: function( options, cb ){

    var self = this;

    //var allIndexes = {};
    var allIndexes = [];

    // Normalise options
    var opt = {};
    if( options.background ) opt.background = true;


    consolelog("Run generateSchemaIndexes for table", self.table );

    consolelog("\nindexGroups for: ", self.table );
    consolelog( require('util').inspect( self._indexGroups, { depth: 10 }  ));
    consolelog("[END]");

    // ***********************************************************************
    // Adding just the keys in indexBase. This is only for the main record,
    // since any indexing of foreign keys in children data is wastage
    // ***********************************************************************
    var keysJustBase = {};
    self._indexGroups.__main.indexBase.forEach( function( indexBaseField ){
      indexBaseField = self._addAllPrefixes( indexBaseField, '' );
      consolelog("indexBaseField after adding ALL prefixes 1: ", indexBaseField );
      keysJustBase[ indexBaseField ] = 1;
    });
    //allIndexes[ group + self._makeSignature( keysJustBase ) ] = { keys: keysJustBase, opt: opt };
    if( Object.keys( keysJustBase ).length != 0 ){
      consolelog("+++Adding searchable key (just base):", keysJustBase );
      allIndexes.push( { keys: keysJustBase, opt: opt, name: 'indexBase' } );
    }

    // Go through each group
    Object.keys( self._indexGroups ).forEach( function( group ){

      var indexGroup = self._indexGroups[ group ];

      consolelog("\n\nDealing with group: ", group, require('util').inspect( indexGroup, { depth: 10 } )  );

      // Sets the field prefix. For __main, it's empty.
      var fieldPrefix = group === '__main' ? '' : group;

      consolelog("fieldPrefix:", fieldPrefix );

      // Goes through every group...
      Object.keys( indexGroup.indexes ).forEach( function( indexName ){

        var indexData = indexGroup.indexes[ indexName ];
        consolelog("ENTRY:", indexName, indexData );

        var k;

        // Make up the index options, mixing opt and indexData.options
        var indexOptions = {};
        for( k in opt ){
          if( !opt.hasOwnProperty( k ) ) continue;
          indexOptions[ k ] = opt[ k ];
        }
        for( k in indexData.options ){
          if( !indexData.options.hasOwnProperty( k ) ) continue;
          indexOptions[ k ] = indexData.options[ k ];
        }

        // Sub-records most not be unique in mongoDB
        // http://stackoverflow.com/questions/35978832/indexing-array-subobject-in-mongodb-causes-duplicate-key-error/35978894#35978894
        if( fieldPrefix != '' ){
          indexOptions.unique = false;
        }

        // Work out indexName with prefix
        var indexNameWithPrefix = fieldPrefix === '' ? indexName: fieldPrefix + "_" + indexName;

        // ******************************
        // Adding keys without base
        // ******************************
        var keysSearchable = {};
        var cleanPathLookup = {};
        Object.keys( indexData.fields ).forEach( function( fieldName ){
          var originalFieldName = fieldName;
          consolelog("fieldPrefix, fieldName: ", fieldPrefix, fieldName );
          var entryData = indexData.fields[ fieldName ];
          fieldName = self._addAllPrefixes( fieldName, fieldPrefix );
          consolelog("indexBaseField after adding ALL prefixes 2: ", fieldName );
          keysSearchable[ fieldName ] = entryData.direction;
          cleanPathLookup[ fieldName ] = originalFieldName;
        });
        consolelog("+++ADDING ", indexNameWithPrefix, keysSearchable, indexData.options );
        allIndexes.push( { name: indexNameWithPrefix, keys: keysSearchable, opt: indexOptions } );

        if( indexGroup.indexBase.length )
          consolelog("indexBase is not empty:", indexGroup.indexBase );

        // **********************************************************************************
        // Making up the same index as keysSearchable, but with indexBase as a starting point
        // **********************************************************************************
        if( indexGroup.indexBase.length && fieldPrefix === '' ){

          var indexName = '';

          // Make up the base bit
          var keysSearchableWithBase = {};
          indexGroup.indexBase.forEach( function( indexBaseField ){
            indexName = indexName + "_" + indexBaseField;
            indexBaseField = self._addAllPrefixes( indexBaseField );
            consolelog("indexBaseField after adding ALL prefixes 3: ", indexBaseField );
            keysSearchableWithBase[ indexBaseField ] = 1;
          });

          // Add the searchable part (borrowing it from above)
          for( k in keysSearchable ){
            if( !keysSearchable.hasOwnProperty( k ) ) continue;
            keysSearchableWithBase[ k ] = keysSearchable[ k ];
            indexName = indexName + "_" + cleanPathLookup[ k ];
          }

          // Make up the index, but only the resulting key is longer than indexBase itself
          // (E.g. indexbase is { workspaceId: 1, personId: 1 } and are indexing personId: the new
          // field will just overwrite one in indexBase, and as a result without this check it would just
          // index indexBase again)
          if( Object.keys( keysSearchableWithBase ).length > indexGroup.indexBase.length ){
            consolelog("+++ADDING searchable key (with base):", 'base_' + indexName, keysSearchableWithBase,indexData.options );

            allIndexes.push( { name: indexName, keys: keysSearchableWithBase, opt: indexOptions } );
          } else {
            consolelog("+++NOT ADDING searchable key (with base), overlapping:", 'base_' + indexName );
          }

        }
      });
    });

    // ***********************************************************************
    // Add index for positionField, keeping into account positionBaseField
    // ***********************************************************************
    var keysForPosition = {};
    self.positionBase.forEach( function( positionBaseField ){
      keysForPosition[ positionBaseField ] = 1;
    });
    keysForPosition[ self.positionField ] = 1;
    consolelog("Keys for position hash:", keysForPosition );
    //allIndexes[ '__main' + self._makeSignature( keysForPosition ) ] = { keys: keysForPosition, opt: opt };
    allIndexes.push( { keys: keysForPosition, opt: opt, name: '_positionIndex' } );

    consolelog("At this point, allIndexes is:" );
    consolelog( allIndexes );

    // Actually make the indexes
    async.eachSeries(
      allIndexes,
      function( item, cb ){
        consolelog("ITEM: ", item );
        self.makeIndex( item.keys, item.name, item.opt, cb );
      },
      cb
    );

  },

  // ******************************************************************
  // ******************************************************************
  // ****              MONGO-SPECIFIC FUNCTIONS                    ****
  // ****              FOR JOINS AND UPPERCASING                   ****
  // ****                                                          ****
  // **** These functions are here to address mongoDb's lack of    ****
  // **** joins by manipulating records' contents when a parent    ****
  // **** record is added, as well as helper functions for         ****
  // **** the lack, in MongoDB, of case-independent searches.      ****
  // ****                                                          ****
  // **** For lack of  case-insensitive search, the layer creates  ****
  // **** __uc__ fields that are uppercase equivalent of 'string'  ****
  // **** fields in the schema.                                    ****
  // ****                                                          ****
  // **** About joins, if you have people and each person can have ****
  // **** several email addresses, when adding an email address    ****
  // **** the "parent" record will have the corresponding          ****
  // **** array in _children updated with the new email address    ****
  // **** added. This means that when fetching records, you        ****
  // **** _automatically_ and _immediately_ have its children      ****
  // **** loaded. It also means that it's possible, in MongoDb,    ****
  // **** to search for fields in the direct children              ****
  // **** I took great care at moving these functions here         ****
  // **** because these are the functions that developers of       ****
  // **** other layer drivers will never have to worry about       ****
  // ****                                                          ****
  // ******************************************************************
  // ******************************************************************

  _deleteUcFieldsAndCleanfromChildren: function( _children ){
    var self = this;

    for( var k in _children ){
      if( !_children.hasOwnProperty( k ) ) continue;
      var child = _children[ k ];
      if( Array.isArray( child ) ){
        for( var i = 0, l = child.length; i < l; i++ ){
          self._deleteUcFields( child[ i ] );
          delete child[ i ]._clean;
        }
      } else {
        self._deleteUcFields( child );
        delete child._clean;
      }
    }
  },

   _deleteUcFields: function( record ){
    for( var k in record ){
      if( k.substr( 0, 6 ) === '__uc__' ) delete record[ k ];
    }
  },


  _addUcFields: function( record ){
    var self = this;

    for( var k in record ){
      if( self._isSearchableAsString( k ) ){
        record[ '__uc__' + k ] = record[ k ].toUpperCase();
      }
    }
  },

  _completeRecord: function( record, cb ){

    var self = this;

    var rnd = Math.floor(Math.random()*100 );
    consolelog( "\n");
    consolelog( rnd, "ENTRY:  _completeRecord for ", self.table, 'record:',  record );

    var recordWithLookups = {};

    // Prepare recordWithLookups, as a copy of record
    for( var k in record ){
      if( !record.hasOwnProperty( k ) ) continue;
      recordWithLookups[ k ] = record[ k ];
    }

    // Each added record needs to be ready for its _children
    recordWithLookups._children = {};

    // Cycle through each lookup child of the current record,
    async.eachSeries(
      Object.keys( self.multipleChildrenTablesHash ),

      function( recordKey, cb ){

        consolelog( rnd, "Working on multiple table:", recordKey);

        consolelog( rnd, recordKey, "Is a multiple table!");

        var childTableData = self.multipleChildrenTablesHash[ recordKey ];

        var childLayer = childTableData.layer;
        //var nestedParams = childTableData.nestedParams;

        consolelog( rnd, "Working on ", childTableData.layer.table );
        consolelog( rnd, "Getting children data (multiple) in child table ", childTableData.layer.table, "for record", recordWithLookups );

        // Get children data for that child table
        // ROOT to _getChildrenData
        self._getChildrenData( recordWithLookups, recordKey, function( err, childData){
          if( err ) return cb( err );

           childData.forEach( function( item ){
            childLayer._addUcFields( item );
          })
          childData._children = {};

          consolelog( rnd, "The childData data is:", childData );

          // Make the record uppercase since it's for a search
          recordWithLookups._children[ recordKey ] = childData;

          // That's it!
          cb( null );
        });
      },

      // End of cycle: function can continue

      function( err ){
        if( err ) return cb( err );

        consolelog( rnd, "Multiple record lookup in insert done. At this point, recordWithLookups is:", recordWithLookups );

        // Cycle through each lookup child of the current record,
        async.eachSeries(
          Object.keys( self.lookupChildrenTablesHash ),

          function( recordKey, cb ){

            consolelog( rnd, "Working on lookup table:", recordKey);

            var childTableData = self.lookupChildrenTablesHash[ recordKey ];

            var childLayer = childTableData.layer;
            var nestedParams = childTableData.nestedParams;

            consolelog( rnd, "Working on ", childTableData.layer.table );
            consolelog( rnd, "Getting children data (lookup) in child table ", childTableData.layer.table," for field ", nestedParams.localField ," for record", recordWithLookups );

            // EXCEPTION: check if the record being looked up isn't the same as the one
            // being added. This is an edge case, but it's nice to cover it
            if( childLayer.table === self.table && recordWithLookups[ self.idProperty ] === recordWithLookups[ recordKey ] ){

              // Make up a temporary copy of the record, to which _children and __uc__ fields
              // will be added
              var t = {};
              for( var k in record ){
                if( !record.hasOwnProperty( k ) ) continue;
                t[ k ] = record[ k ];
              }
              childLayer._addUcFields( t );
              t._children = {};
              recordWithLookups._children[ recordKey ] = t;

              return cb( null );
            }

            // Get children data for that child table
            // ROOT to _getChildrenData
            self._getChildrenData( recordWithLookups, recordKey, function( err, childData){
              if( err ) return cb( err );

              if( childData ){

                childLayer._addUcFields( childData );
                childData._children = {};

                consolelog( rnd, "The childData data is:", childData );

                 // Make the record uppercase since it's for a search
                recordWithLookups._children[ recordKey ] = childData;
              }

              // That's it!
              cb( null );
            });
          },

          // End of cycle: function can continue

          function( err ){
            if( err ) return cb( err );

            consolelog( rnd, "completeRecord done. At this point, recordWithLookups is:", recordWithLookups );

            cb( null, recordWithLookups );
          }
        );

      }
    );

  },

  _makeUpdateAndUnsetObjectWithLookups: function( updateObject, unsetObject, cb ){
    var self = this;
    var k;

    var rnd = Math.floor(Math.random()*100 );
    consolelog( "\n");
    consolelog( rnd, "ENTRY:  _makeUpdateAndUnsetObjectWithLookups for ", self.table, 'updateObject:',  updateObject );

    // Make a copy of the original updateObject. The copy will be
    // enriched and will then be returned
    var updateObjectWithLookups = {};
    for( k in updateObject ){
      if( !updateObject.hasOwnProperty( k ) ) continue;
      updateObjectWithLookups[ k ] = updateObject[ k ];
    }
    var unsetObjectWithLookups = {};
    for( k in unsetObject ){
      if( !unsetObject.hasOwnProperty( k ) ) continue;
      unsetObjectWithLookups[ k ] = unsetObject[ k ];
    }

    // This is only for aesthetic purposes. In an update, the update object
    // "is" the record (although it might be a partial version of it)
    var record = updateObject;

    // Cycle through each lookup child of the update object
    async.eachSeries(
      Object.keys( self.lookupChildrenTablesHash ),

      function( recordKey, cb ){

        //consolelog( rnd, "Checking that field", recordKey, "is actually a lookup table...");
        // ...and that it's defined in the record itself (it might be a partial update)
        //if( ! self.lookupChildrenTablesHash[ recordKey ] ){
        if( ! self.lookupChildrenTablesHash[ recordKey ] || typeof( record[ recordKey ] ) === 'undefined' ){
          //consolelog( rnd, "It isn't! Ignoring it...");
          return cb( null );
        } else {
          consolelog( rnd, recordKey, "Is a lookup table!");

          var childTableData = self.lookupChildrenTablesHash[ recordKey ];

          var childLayer = childTableData.layer;
          var nestedParams = childTableData.nestedParams;

          consolelog( rnd, "Working on ", childTableData.layer.table );
          consolelog( rnd, "Getting children data in child table ", childTableData.layer.table," for field ", nestedParams.localField ," for record", record );

          // Get children data for that child table
          // ROOT to _getChildrenData
          self._getChildrenData( record, recordKey, function( err, childData){
            if( err ) return cb( err );

            if( childData ){

              // Add uppercase fields
              childLayer._addUcFields( childData );

              // Add _children to childData, as it's expected a lot of times
              childData._children = {};

              // Make up the updateObjectWithLookup object with the new childData
              updateObjectWithLookups[ '_children.' + recordKey ] = childData;

              // That's it!
              cb( null );
            } else {

              // Watch out: protected fields mustn't be overwritten by an update
              if( ! self.schema.structure[ recordKey ].protected ){

                // Make up the unsetObjectWithLookup object with the new childData
                unsetObjectWithLookups[ '_children.' + recordKey ] = 1;
              }

              // That's it!
              cb( null );

            }

          });

        }
      },

      // End of cycle: function can continue

      function( err ){
        if( err ) return cb( err );

        cb( null, updateObjectWithLookups, unsetObjectWithLookups );
      }
    );

  },


  /* This function takes a layer, a record and a child table, and
     returns children data about that field.
     The return value might be an array (for 1:n relationships) or a
     straight object (for lookups);

     NOTE: In mongoDbLayer, this function is only really used for lookups
     as 1:n sub-records are obviously only added to the master one when the
     sub-record is added.
  */
  _getChildrenData: function( record, field, cb ){

    var self = this;

    var mongoSelector;

    // The layer is the object from which the call is made
    var layer = this;

    var rnd = Math.floor(Math.random()*100 );
    consolelog( "\n");
    consolelog( rnd, "ENTRY: _getChildrenData for ", field, ' => ', record );
    //consolelog( rnd, "Comparing with:", Object.keys( rootTable.autoLoad ) );

    var childTableData = self.childrenTablesHash[ field ];

    switch( childTableData.nestedParams.type ){

      case 'multiple':
        consolelog( rnd, "Child table to be considered is of type MULTIPLE" );

        // JOIN QUERY (direct)
        mongoSelector = {};
        Object.keys( childTableData.nestedParams.join ).forEach( function( joinKey ){
          var joinValue = childTableData.nestedParams.join[ joinKey ];
          mongoSelector[ joinKey ] = record[ joinValue ];
        });

        consolelog( rnd, "Running the select with selector:", mongoSelector, "on table", childTableData.layer.table );

        // Runs the query, which will get the children element for that
        // child table depending on the join
        // TODO POSITIONING: Add sorting by positioning here, so that children will have the
        // right order
        childTableData.layer.collection.find( mongoSelector).project(childTableData.layer._projectionHash ).toArray( function( err, res ){
          if( err ) return cb( err );

          var deleteId = typeof( childTableData.layer._fieldsHash._id ) === 'undefined';
          res.forEach( function( item ) {
            if( deleteId ) delete item._id;
            delete item._clean;
          });
          cb( null, res );
        });
      break;

      case 'lookup':

        consolelog( rnd, "Child table to be considered is of type LOOKUP" );

        // JOIN QUERY (direct)
        mongoSelector = {};
        mongoSelector[ childTableData.nestedParams.layerField ] = record[  childTableData.nestedParams.localField ];

        consolelog( rnd, "Running the select with selector:", mongoSelector, "on table", childTableData.layer.table );

        // Runs the query, which will get the children element for that
        // child table depending on the join
        childTableData.layer.collection.find( mongoSelector).project( childTableData.layer._projectionHash ).toArray( function( err, res ){
          if( err ) return cb( err );

          // Return null if it's a lookup and there are no results
          if( res.length === 0 ){
            return cb( null, null );
          }
          var r = res[ 0 ];

          if( typeof( childTableData.layer._fieldsHash._id ) === 'undefined' ) delete r._id;
          delete r._clean;

          return cb( null, r );
        });
      break;
    }
  },



  /*
   This function will update each parent record so that it contains
   up-to-date information about its children.
   If you update a child record, any parent containing a reference to it
   will need to be updated.
   This is built to be _fast_: there is only one extra update for each
   parent table.
  */
  _updateParentsRecords: function( params, cb ){

    var self = this;
    var layer = this;
    var selector;
    var updateObject, unsetObject, relativeUnsetObject, relativeUpdateObject;
    var id, k, prefix, mongoParameters;
    var conditions, options;

    // Paranoid checks and sane defaults for params
    if( typeof( params ) !== 'object' || params === null ) params = {};

    var rnd = Math.floor(Math.random()*100 );
    consolelog( "\n");
    consolelog( rnd, "ENTRY: _updateParentsRecords on table", self.table );
    consolelog( rnd, 'Params:' );
    consolelog( require('util').inspect( params, { depth: 10 } ) );

    consolelog( rnd, "Cycling through: ", self.parentTablesArray );

    // Cycle through each parent of the current layer
    async.eachSeries(
      self.parentTablesArray,
      function( parentTableData, cb ){

        consolelog( rnd, "Working on ", parentTableData.layer.table );

        var parentLayer = parentTableData.layer;
        var nestedParams = parentTableData.nestedParams;

        // Figure out the field name, relative to _children.XXXXX
        // - For multiple, it will just be the table's name or prop
        // - For lookups, it will be the localField value in nestedParams or prop
        var field;
        switch( nestedParams.type ){
          case 'multiple': field = nestedParams.prop || self.table; break;
          case 'lookup'  : field = nestedParams.prop || nestedParams.localField; break;
          default        : return cb( new Error("type needs to be 'lookup' or 'multiple'") );
        }
        consolelog( "FIELD:", field );
        consolelog( "PARAMETERS:", nestedParams );
        consolelog( "PARAMETERS TYPE:", nestedParams.type );

        consolelog( rnd, "fielddd for ", nestedParams.type, "is:", field );

        /* THREE CASES:
          * CASE #1: Insert
          * CASE #2: Update
          * CASE #3: Delete

                              *** DO NOT OPTIMISE THIS CODE ***

          I REPEAT: DO NOT OPTIMISE THIS CODE.
          This code has the potential to become the most uncomprehensible thing ever written. Worse than Perl.
          Optimising here is simple -- and even tempting. DON'T. Clarity here is MUCH more important than
          terseness.
        */

        // CASE #1 -- INSERT

        if( params.op === 'insert' ){

          if( nestedParams.type === 'multiple' ){

            consolelog( rnd, "CASE #1 (insert, multiple)" );

            // Assign the parameters
            var record = params.record;

            // JOIN QUERY (reversed, look for parent)
            var mongoSelector = {};
            Object.keys( nestedParams.join ).forEach( function( joinKey ){
              mongoSelector[ nestedParams.join[ joinKey ] ] = record[ joinKey ];
            });
            mongoSelector._clean = true;

            // The updateRecord variable is the same as record, but with uc fields and _children added
            var insertRecord = {};
            for( k in record ){
              if( ! record.hasOwnProperty( k ) ) continue;
              insertRecord[ k ] = record[ k ];
            }
            self._addUcFields( insertRecord );
            insertRecord._children = {};

            updateObject = { '$push': {} };
            updateObject.$push[ '_children' + '.' + field ] = { '$each': [ insertRecord ], '$slice': -1000 };

            consolelog( rnd, "The mongoSelector is:", mongoSelector  );
            consolelog( rnd, "The update object is: ");
            consolelog( require('util').inspect( updateObject, { depth: 10 } ) );

            parentLayer.collection.update( mongoSelector, updateObject, function( err, total ){
              if( err ) return cb( err );
              // This check s commented out as there might (or there WILL) be parent tables without
              // the matching id
              //if( total === 0 ) return cb( new Error("Record not found while adding entry to child table") );
              consolelog( rnd, "Record inserted in sub-array: ", total );

              cb( null );

            });

          // noop
          } else if( nestedParams.type === 'lookup' ){

            return cb( null );
          }

        // CASE #2 -- UPDATE

        } else if( params.op === 'updateOne' || params.op === 'updateMany' ){

          if( params.op === 'updateOne' ){

            consolelog( rnd, "CASE #2 (updateOne)", params.op );

            // Assign the parameters
            id = params.id;
            updateObject = params.updateObject;
            unsetObject = params.unsetObject;

            prefix = '_children.' + field + ( nestedParams.type === 'multiple' ?  '.$.' : '.' ) ;

            // Make up relative update objects based on the original ones,
            // with extra path added
            relativeUpdateObject = {};
            for( k in updateObject ){
              if( !updateObject.hasOwnProperty( k ) ) continue;
              relativeUpdateObject[ prefix + k ] = updateObject[ k ];
            }
            relativeUnsetObject = {};
            for( k in unsetObject ){
              if( !unsetObject.hasOwnProperty( k ) ) continue;
              relativeUnsetObject[ prefix + k ] = unsetObject[ k ];
            }

            selector = {};
            selector[ '_children.' + field + "." + self.idProperty ] = id;
            selector._clean = true;
            consolelog( rnd, "SELECTOR:" );
            consolelog( rnd, selector );

            var u = {};
            if( Object.keys( relativeUpdateObject ).length ) u.$set = relativeUpdateObject ;
            if( Object.keys( relativeUnsetObject ).length ) u.$unset = relativeUnsetObject;
            parentLayer.collection.update( selector, u, { multi: true }, function( err, total ){
              if( err ) return cb( err );

              consolelog( rnd, "Updated:", total, "records" );

              return cb( null );

            });
          }

          if( params.op === 'updateMany' ){

            consolelog( rnd, "CASE #2 (updateMany)", params.op );

            // Sorry, can't. MongoDb bug #1243
            if( nestedParams.type === 'multiple' ){
              return cb( new Error("You cannot do a mass update of a table that has a father table with 1:n relationship with it. Ask Mongo people to fix https://jira.mongodb.org/browse/SERVER-1243") );
            }

            // Assign the parameters
            conditions = params.conditions;
            updateObject = params.updateObject;
            unsetObject = params.unsetObject;

            // Make up parameters from the passed filters
            try {
              mongoParameters = parentLayer._makeMongoParameters( conditions, {}, field );
              // var mongoParameters = parentLayer._makeMongoParameters( filters );
            } catch( e ){
              return cb( e );
            }

            consolelog( rnd,  "mongoParameters:" );
            consolelog( mongoParameters.querySelector );

            prefix = '_children.' + field + ( nestedParams.type === 'multiple' ?  '.$.' : '.' );

            // Make up relative update objects based on the original ones,
            // with extra path added
            relativeUpdateObject = {};
            for( k in updateObject ){
              if( !updateObject.hasOwnProperty( k ) ) continue;
              relativeUpdateObject[ prefix + k ] = updateObject[ k ];
            }
            relativeUnsetObject = {};
            for( k in unsetObject ){
              if( !unsetObject.hasOwnProperty( k ) ) continue;
              relativeUnsetObject[ prefix + k ] = unsetObject[ k ];
            }

            consolelog( rnd,  "updateObject:" );
            consolelog( relativeUpdateObject );

            var u = {};
            if( Object.keys( relativeUpdateObject ).length ) u.$set = relativeUpdateObject;
            if( Object.keys( relativeUnsetObject ).length ) u.$unset = relativeUnsetObject;
            parentLayer.collection.update( mongoParameters.querySelector, u, { multi: true }, function( err, total ){
              if( err ) return cb( err );

              consolelog( rnd, "Updated:", total.result.n, "records" );

              return cb( null );
            });
          }

        // CASE #3 -- DELETE
        } else if( params.op === 'deleteOne' || params.op === 'deleteMany' ){

          if( params.op === 'deleteOne' ){

            consolelog( rnd, "CASE #3 (deleteOne)", params.op );

            // Assign the parameters
            id = params.id;

            updateObject = {};

            selector = {};
            selector[ '_children.' + field + "." + self.idProperty ] = id;
            selector._clean = true;

            // It's a lookup field: it will assign an empty object
            if( nestedParams.type === 'lookup' ){
              updateObject.$set = {};
              updateObject.$set[ '_children.' + field ] = {};

            // It's a multiple one: it will $pull the element out
            } else {
              updateObject.$pull = {};

              var pullData = {};
              pullData[ self.idProperty  ] =  id ;
              updateObject.$pull[ '_children.' + field ] = pullData;
            }

            consolelog( rnd, "Selector:");
            consolelog( selector );
            consolelog( rnd, "UpdateObject:");
            consolelog( updateObject );

            parentLayer.collection.update( selector, updateObject, { multi: true }, function( err, total ){
              if( err ) return cb( err );

              consolelog( rnd, "Deleted", total, "sub-records" );

              return cb( null );

            });
          }

          if( params.op === 'deleteMany' ){

            consolelog( rnd, "CASE #3 (deleteMany)", params.op );

            // No way. Too untested.
            if( nestedParams.type === 'multiple' ){
              return cb( new Error("Sorry, this code is just too untested to do that right now") );
            }


            // Assign the parameters
            conditions = params.conditions;
            consolelog("Making filter...", conditions, field );

            // Make up parameters from the passed filters
            try {
              mongoParameters = parentLayer._makeMongoParameters( conditions, options, field );
            } catch( e ){
              return cb( e );
            }
            selector = { $and: [ { _clean: true }, mongoParameters.querySelector ] };

            // Make up parameters from the passed filters
            var mongoParametersForPull;
            try {
              mongoParametersForPull = parentLayer._makeMongoParameters( conditions, options, field, true );
            } catch( e ){
              return cb( e );
            }

            consolelog("mongoParameters:", mongoParameters );
            consolelog("mongoParametersForPull:", mongoParametersForPull );

            // The update object will depend on whether it's a push or a pull
            updateObject = {};

            // It's a lookup field: it will assign an empty object
            if( nestedParams.type === 'lookup' ){

              consolelog("It's a lookup!");

              updateObject.$set = {};
              updateObject.$set[ '_children.' + field ] = {};

              consolelog("Query Selector:", mongoParameters.querySelector );
              consolelog("Update object:", updateObject );

              parentLayer.collection.update( selector, updateObject, { multi: true }, function( err, total ){
                if( err ) return cb( err );
                consolelog( rnd, "deleted", total, "sub-records" );

                return cb( null );
              });

            // It's a multiple one: it will $pull the elementS (with an S, plural) out
            } else {

              consolelog("It's a multiple!");

              updateObject.$pull = {};
              updateObject.$pull[ '_children.' + field ] = mongoParametersForPull.querySelector;

              consolelog("Query Selector:", mongoParameters.querySelector );
              consolelog("Update object:", updateObject );

              parentLayer.collection.update( mongoParameters.querySelector, updateObject, { multi: true }, function( err, total ){
                if( err ) return cb( err );
                consolelog( rnd, "deleted", total, "sub-records" );

                return cb( null );
              });
            }

          }

        // CASE #? -- This mustn't happen!
        } else {
          consolelog( rnd, "WARNING?!? params.op and nestedParams.type are:", params.op, nestedParams.type );

          cb( null );
        }

      },

      function( err ){
        if( err ) return cb( err );
        consolelog( rnd, "EXIT: End of function ( _updateParentsRecords)" );
        cb( null );
      }
    );

  },

});

// The default id maker
MongoMixin.makeId = function( id, cb ){
  if( id === null ){
    cb( null, makeObjectId() );
  } else {
    cb( null, makeObjectId( id ) );
  }
};

exports = module.exports = MongoMixin;
