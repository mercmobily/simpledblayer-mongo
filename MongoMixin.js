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

var consolelog = function(){
  console.log.apply( console, arguments );
}

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

    //consolelog('\n\nINITIATING ', self );

    Object.keys( self.schema.structure ).forEach( function( field ) {
      //consolelog("FIELD: ", field );
      var entry = self.schema.structure[ field ];
      //consolelog("ENTRY: ", entry );
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
          //consolelog("SEARCHABLE HASH: ", self._searchableHash, field );
          if( !self._searchableHash[ field ] ){
            // FIXME: reinstate this, after automatically adding the necessary indexes
            //throw( new Error("Field " + field + " is not searchable" ) );
          }

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
      // consolelog( "FINAL SELECTOR" );        
      // consolelog( require('util').inspect( finalSelector, { depth: 10 } ) );        

    };    

    // make sortHash
    // If field is searchable, swap field names for _uc_ equivalent so that
    // sorting happens regardless of upper or lower case
    var sortHash = {};
    for( var field  in filters.sort ) {
      // if( self._searchableHash[ field ] )  var finalField = '__uc__' + field; else finalField = field;
      sortHash[ field ] = filters.sort[ field ];
    }
    //consolelog( "FINAL SORTHASH", self.table );        
    //consolelog( require('util').inspect( sortHash, { depth: 10 } ) );        

    return { querySelector: finalSelector, sortHash: sortHash };
  }, 


  _toUpperCaseRecord: function( record ){
    for( var k in record ){
      if( typeof( record[ k ] ) === 'string' ) record[ k ] = record[ k ].toUpperCase();
    }
  },


  /*
    This function just takes a record, and calls _completeRecord
    so that both _children and _searchData are filled in.
  */
  completeRecord: function( record, cb ){

    var self = this;

    var rnd = Math.floor(Math.random()*100 );

    consolelog( "\n");
    consolelog( rnd, "ENTRY: _completeRecord for ",  self.table, ' => ', record );

    // ROOT to _completeRecord
    self._completeRecord( record, { upperCase: false, field: '_children', ifAutoload: true }, self.table, self, function( err ){
      if( err ) return cb( err );

      //self._completeRecord( record, { upperCase: true, field: '_searchData', ifAutoload: false }, self.table, function( err ){
        //if( err ) return cb( err );

        consolelog( rnd, "EXIT: record is:", require('util').inspect( record, { depth: 8 } ) );
        cb( null );
      //});
    });
  },
         

  /* ***This function CHANGES record!***
     This function goes through every child table, and runs
     _getChildrenData for each one of them, *changing* record
     so that it includes the child table's information.
     Basically, if `contacts` is 1:n with `addresses`, it will
     populate record._children.addresses[] with every corresponding address.
     
  */ 
  _completeRecord: function( record, params, cb ){

    var self = this;

    var rnd = Math.floor(Math.random()*100 );

    consolelog( "\n");
    consolelog( rnd, "ENTRY: _completeRecord for ",  self.table, ' => ', record, ", params:", params );

    // The layer is the object from which the call is made
    var layer = this;

    // Path needs to be an object
    if( typeof( path ) !== 'string' ){
      return cb( new Error( "Parameter path of _getChildrenData must be a string") );
    }

    consolelog( rnd, "Cycling through: ", layer.childrenTablesHash );

    // This should go in the right spot, right here. And not IN the cycle zapping
    // the previous values every time. Oh boy.
    record[ params.field ] = {};

    async.eachSeries(
      Object.keys( layer.childrenTablesHash ),
      function( childTableKey, cb ){
    
        var childTableData = layer.childrenTablesHash[ childTableKey ];
 
        consolelog( rnd, "Working on ", childTableData.layer.table );

        // Since there can be several fields pointing to the same
        // lookup table (e.g. `personId`, `nextOfKinId` in the same table),
        // nestedParams should contain `loadAs` which is the spot where the information
        // will be placed. If not, it falls back to the layer's name.
        // Note that it cannot fallback to the record's name, since the join
        // can have several elements and would be ambiguous.
        //var loadAs = childTableData.nestedParams.loadAs ? childTableData.nestedParams.loadAs : childTableData.nestedParams.layer.table;

        consolelog( rnd, "Getting children data in child table ", childTableData.layer.table," for record", record );

        // Runs _getChildrenData for the found child table

        var field;
        switch( childTableData.nestedParams.type ){
          case 'multiple': field = childTableData.layer.table; break;
          case 'lookup'  : field = childTableData.nestedParams.parentField; break;
          default        : return cb( new Error("The options parameter must be a non-null object") ); break;
        }
     
        // Check if the key needs to be worked on
        if( !childTableData.nestedParams.autoLoad ){
          consolelog( rnd, "Child key doesn't have autoLoad turned on, skipping..." );
          return cb( null );
        }

        layer._getChildrenData( record, field, params, function( err, childrenData ){
          if( err ) return cb( err );
          
          consolelog( rnd, "Extra information will be stored in records[", params.field , " ] where record is", record );


          switch( childTableData.nestedParams.type ){
            case 'lookup':
              consolelog( rnd, "Its a lookup. Doing: record[ params.field ][ loadAs ] = childrenData[ loadAs ] ");
              consolelog( rnd, "Note that childrenData is:", childrenData );

              var loadAs = childTableData.nestedParams.parentField;
              consolelog( rnd, "Entries will be placed in key:", loadAs );
              if( childrenData[ loadAs ] ) record[ params.field ][ loadAs ] = childrenData; 
            break;

            case 'multiple':
              consolelog( rnd, "Its a multiple. Doing: record[ params.field ][ loadAs ] = childrenData");

              var loadAs = childTableData.layer.table;
              consolelog( rnd, "Entries will be placed in key:", loadAs );
              if( childrenData.length ) record[ params.field ][ loadAs ] = childrenData; 
            break;

            default:
              cb( new Error( "Parameter 'type' needs to be 'lookup' or 'multiple' " ) );
            break;
          }

          consolelog( rnd, "Record, which should be growing, is now: ", record );

          cb( null );
        });
      },
      function( err ){
        if( err ) return cb( err );
       
				consolelog( rnd, "EXIT: record is:", require('util').inspect( record, { depth: 8 } ) );
 
        cb( null );
      }
    );

  },
  

  /* This function takes a layer, a record and a child table, and
     makes sure that record._children.field is populated with
     children's information.
     Once each sub-record is added to record._children.childTable, the
     method _completeRecord() is called to make sure that the record's
     own _children are fully filled
  */
  _getChildrenData: function( record, field, params, cb ){

    var self = this;

    // The layer is the object from which the call is made
    var layer = this;

    var rnd = Math.floor(Math.random()*100 );
    consolelog( "\n");
    consolelog( rnd, "ENTRY: _getChildrenData for ", field, ' => ', record );
    //consolelog( rnd, "Comparing with:", Object.keys( rootTable.autoLoad ) );

    // Little detail forgotten by accident
    var resultObject;

    // Paranoid check on params, want it as an object
    if( typeof( params ) !== 'object' || params === null ) params = {};

    var childTableData = layer.childrenTablesHash[ field ]; 


    // If it's a lookup, it will be v directly. This will cover cases where a new call
    // is made straight on the lookup
    switch( childTableData.nestedParams.type ){

      case 'multiple':
        consolelog( rnd, "Child table to be considered is of type MULTIPLE" );
        resultObject = [];
      break;

      case 'lookup':
        consolelog( rnd, "Child table to be considered is of type LOOKUP" );
        resultObject = {};
      break;
    }
    
    // Checking that rootTable actually does want this to be scanned
    if( params.field === '_children' ){
      if( ! childTableData.nestedParams.autoLoad ){
        consolelog( rnd, "autoLoad for _children not turned on, getting out!" );
        return cb( null, resultObject );
      }
    }

    // TODO: change the query so that the lookup gets a different query

    // Make the conditions array to make the right query based on the join
    var andConditionsArray = [];
    Object.keys( childTableData.nestedParams.join ).forEach( function( joinKey ){
      var joinValue = childTableData.nestedParams.join[ joinKey ];
      andConditionsArray.push( { field: joinKey, type: 'eq', value: record[ joinValue ] } );
    });

    consolelog( rnd, "Running the select...", andConditionsArray );

    // Runs the query, which will get the children element for that
    // child table depending on the join
    childTableData.layer.select( { conditions: { and: andConditionsArray } }, function( err, res, total ){
      if( err ) return cb( err );

      consolelog( rnd, "Records fetched:", total, res );
   
      // For each result, add them to resultObject
      async.eachSeries(
        res,
    
        function( item, cb ){

          consolelog( rnd, "Considering item:", item );
   
          // Make the record uppercase if so required
          if( params.upperCase ){
            consolelog( rnd, "UpperCasing the item as requested by params");
            self._toUpperCaseRecord( item );
          }

          // Assign the loaded item to the resultObject
          // making sure that it's in the right spot (depending on the type)
          switch( childTableData.nestedParams.type ){
            case 'lookup':
             //var loadAs = childTableData.nestedParams.loadAs ? childTableData.nestedParams.loadAs : childTableData.nestedParams.layer.table;
             var loadAs = childTableData.nestedParams.parentField;
             consolelog( rnd, "Item is a lookup, assigning resultobject[ ", loadAs,' ] to ', resultObject );
             resultObject = item;
            break;
            case 'multiple':
              consolelog( rnd, "Item is of type multiple, pushing it to", resultObject );
              resultObject.push( item );
            break;
            default:
              cb( new Error( "Parameter 'type' needs to be 'lookup' or 'multiple' " ) );
            break;
          };

          consolelog( rnd, "resultObject after the cure is:", resultObject );

          cb( null );
            
        },

        function( err ){
          if( err ) return cb( err );

          consolelog( rnd, "EXIT: End of function. Returning:", require('util').inspect( resultObject, { depth: 5 }  ) );

          cb( null, resultObject );
        }
      ) // async.series

    })
  },


  /*
    This function will make sure that every lookup field is actually looked up
    and placed in the database.
    It's important to do this, as it's the "initial" setup of the record being written.
  */
  _updateSelfWithLookups: function( record, params, cb ){

    var layer = this;
    var self = this;
   
    // Paranoid checks and sane defaults for params
    if( typeof( params ) !== 'object' || params === null ) params = {};
    if( typeof( params.field ) !== 'string' ) params.field = '_children';

    var rnd = Math.floor(Math.random()*100 );
    consolelog( "\n");
    consolelog( rnd, "ENTRY: _updateSelfWithLookups for ", layer.table, ' => ', record );

    consolelog( rnd, "Cycling through: ", layer.lookupChildrenTablesHash,", the children of tyle 'lookup'"  );

    // Cycle through each lookup child of the current layer
    async.eachSeries(
      Object.keys( layer.lookupChildrenTablesHash ),
      function( childTableKey, cb ){

        var childTableData = layer.lookupChildrenTablesHash[ childTableKey ];

        consolelog( rnd, "Working on ", childTableData.layer.table );
        consolelog( rnd, "Getting children data in child table ", childTableData.layer.table," for record", record );

        var childLayer = childTableData.layer;
        var nestedParams = childTableData.nestedParams;

        
        // Check if the key needs to be worked on
        if( !nestedParams.autoLoad ){
          consolelog( rnd, "Child key doesn't have autoLoad turned on, skipping..." );
          return cb( null );
        }

        // Get children data for that child table
        // ROOT to _getChildrenData
        layer._getChildrenData( record, nestedParams.parentField, params, function( err, childData){
          if( err ) return cb( err );

          consolelog( rnd, "The childData data is:", childData );

          // Make up the selectors. The first one is a simpledbschema selector, needed for
          // the layer's select command. The second one is a straight MongoDb selector, needed for
          // the update (made using the Mogodb driver directly)
          var mongoSelector = {}; mongoSelector[ nestedParams.layer.idProperty ] = record[ nestedParams.layer.idProperty ];
          consolelog( rnd, "Using selector for the query: ", mongoSelector );

          //var selector = { conditions: { and: [  ] } };
          //var mongoSelector = layer._makeMongoParameters( selector ).querySelector;

          // Set loadAs to the right value depending on the child type (lookup or multuple)
          var loadAs = nestedParams.parentField;

          consolelog( rnd, "loadAs is:", loadAs );

          // Create the update object for mongoDb
          var updateObject = { '$set': {} };
          updateObject[ '$set' ] [ params.field + '.' + loadAs ] = childData;

          consolelog( rnd, "Updating 1: " , layer.table," with selector: ", mongoSelector, "and update object:", updateObject );

          // Update the collection with the new info,
          layer.collection.update( mongoSelector, updateObject, function( err, total ){
            if( err ) return cb( err );

            consolelog( rnd, "Updated: ", total );
            cb( null );
          });
        });

      },
      function( err ){
        if( err ) return cb( err );

        consolelog( rnd, "EXIT: End of function." );

        cb( null );
      }
    );
  },

 
  /*
   This function will update each parent record so that it contains
   up-to-date information about the child.
   If you update a child record, any parent containing a reference to it
   will need to be updated.
   Note that the function will run a different `update` operation depending
   on whether the parent is a 1:n relationship, or it's a lookup.
  */
  _updateParentsRecords: function( record, params, cb ){

    var self = this;
    var layer = this;

    // Paranoid checks and sane defaults for params
    if( typeof( params ) !== 'object' || params === null ) params = {};
    if( typeof( params.field ) !== 'string' ) params.field = '_children';

    var rnd = Math.floor(Math.random()*100 );
    consolelog( "\n");
    consolelog( rnd, "ENTRY: _updateParentsRecords for ", layer.table, ' => ', record );

    consolelog( rnd, "Cycling through: ", layer.parentTablesArray );

    // Cycle through each parent of the current layer
    async.eachSeries(
      layer.parentTablesArray,
      function( parentTableData, cb ){
    
        consolelog( rnd, "Working on ", parentTableData.layer.table );
 
        var parentLayer = parentTableData.layer;
        var nestedParams = parentTableData.nestedParams;

        // Figure out what to pass as the second parameter of _getChildrenData: 
        // - For multiple, it will just be the table's name
        // - For lookups, it will be the parentField value in nestedParams
        var field;
        switch( nestedParams.type ){
          case 'multiple': field = layer.table; break;
          case 'lookup'  : field = nestedParams.parentField; break;
          default        : return cb( new Error("The options parameter must be a non-null object") ); break;
        }
        console.log( rnd, "field for ", nestedParams.type, "is:", field );

        /* FOUR CASES:
          * CASE #1 Insert into a multiple one   -> Push into _children.addresses[] where the (reversed) join is satisfied
          * CASE #2 Insert into lookup -> Do nothing
          * CASE #3 Update into a multiple one   -> Update to new info where _children.addresses.id is the same as record.id
          * CASE #4 Update into lookup -> Update the new info where _children.country.id is the same as record.id
        */

        // CASE #1
        // #831: change all joinKey with the path info, change the $push with the path info

        if( params.op === 'insert' && nestedParams.type === 'multiple' ){

          consolelog( rnd, "CASE #1 (insert, multiple)" );

          var andConditionsArray = [];
          Object.keys( nestedParams.join ).forEach( function( joinKey ){
            andConditionsArray.push( { field: nestedParams.join[ joinKey ], type: 'eq', value: record[ joinKey ] } );
          });

          var updateObject = { '$push': {} };
          updateObject[ '$push' ] [ params.field + '.' + field ] = record;

          var selector = { conditions: { and: andConditionsArray } }; 
          var mongoSelector = parentLayer._makeMongoParameters( selector ).querySelector; 

          consolelog( rnd, "The mongoSelector is:", mongoSelector  );
          consolelog( rnd, "The update object is: ", updateObject);
          consolelog( rnd, "Running the update...", andConditionsArray );

          parentLayer.collection.update( mongoSelector, updateObject, { multi: true }, function( err, total ){

            consolelog( rnd, "Updated:", total, "records" );
            // End of story
            cb( null );

           
          });

        // CASE #2
        // noop

        // CASE #3
        // #831: andConditionsArray needs to be prefixed with the father's table, the update object too

        } else if( params.op === 'update' && nestedParams.type === 'multiple' ){

          consolelog( rnd, "CASE #3 (update, multiple)" );

          var andConditionsArray = [];
          andConditionsArray.push( { field: params.field + '.' + field + '.' + parentLayer.idProperty, type: 'eq', value: record[ parentLayer.idProperty ] } );

          var updateObject = { '$set': {} };
          updateObject[ '$set' ] [ params.field + '.' + field + '$' ] = record;

          var selector = { conditions: { and: andConditionsArray } }; 
          var mongoSelector = parentLayer._makeMongoParameters( selector ).querySelector; 

          consolelog( rnd, "The mongoSelector is:", mongoSelector  );
          consolelog( rnd, "The update object is: ", updateObject);
          consolelog( rnd, "Running the update...", andConditionsArray );

          parentLayer.collection.update( mongoSelector, updateObject, { multi: true }, function( err, total ){

            consolelog( rnd, "Updated:", total, "records" );
            // End of story
            cb( null );

           
          });

        // CASE #4
        // #831:

        // TODO: TEST THIS, NEVER TESTED
        } else if( params.op === 'update' && nestedParams.type === 'lookup' ){

          consolelog( rnd, "CASE #4 (update, lookup)" );

          var andConditionsArray = [];
          andConditionsArray.push( { field: params.field + '.' + field + '.' + parentLayer.idProperty, type: 'eq', value: record[ parentLayer.idProperty ] } );

          var updateObject = { '$set': {} };
          updateObject[ '$set' ] [ params.field + '.' + field ] = record;

          var selector = { conditions: { and: andConditionsArray } }; 
          var mongoSelector = parentLayer._makeMongoParameters( selector ).querySelector; 

          consolelog( rnd, "The mongoSelector is:", mongoSelector  );
          consolelog( rnd, "The update object is: ", updateObject);
          consolelog( rnd, "Running the update...", andConditionsArray );

          parentLayer.collection.update( mongoSelector, updateObject, { multi: true }, function( err, total ){

            consolelog( rnd, "Updated:", total, "records" );
            // End of story
            cb( null );

           
          });




        } else {

                    

          cb( null );
        }

        

        // TODO: Reinstate this
        // If this is only to load in autoload, and autoload is off for this join,
        // then quit it here
        //if( ! nestedParams.autoload && params.ifAutoload ){
        //  consolelog( rnd, "autoload is off and ifAutoLoad is on: aborting this one..." );
        //  return cb( null );
        //}

        // Work out the select conditions based on the join, to fetch the
        // relevant parent records
        //var andConditionsArray = [];
        //Object.keys( nestedParams.join ).forEach( function( joinKey ){
        //  andConditionsArray.push( { field: nestedParams.join[ joinKey ], type: 'eq', value: record[ joinKey ] } );
        //});

        /*

        consolelog( rnd, "Telling" , parentLayer.table, "to update refs for", layer.table, "for records matchng", andConditionsArray );
   
        // condition[ { 'params.field' + field + parentLayer.idProperty  } ] = record.
 
        // Make up the selectors. The first one is a simpledbschema selector, needed for
        // the layer's select command. The second one is a straight MongoDb selector, needed for
        // the update (made using the Mogodb driver directly)
        var selector = { conditions: { and: [ { 'params.field' + field + parentLayer.idProperty  }  ]  } }; 
        var mongoSelector = parentLayer._makeMongoParameters( selector ).querySelector; 
        consolelog( rnd, "The Mongo selector is: ", mongoSelector );
  
        var updateObject = { '$set': {} };
        updateObject[ '$set' ] [ params.field + '.$.' + field ] = record;

        consolelog( rnd, "Making the mongo update for field ", params.field + '.' + field );

        consolelog( rnd, "Running the update...", andConditionsArray );


        parentLayer.collection.update( mongoSelector, updateObject, { multi: true }, function( err, total ){
        
          consolelog( rnd, "Updated:", total, "records" );
          // End of story
          cb( null );

        });
          
        */

      },
    
      function( err ){
        if( err ) return cb( err );
        consolelog( rnd, "EXIT: End of function." );
        cb( null );
      }
    );


  },


 
  _updateCacheDelete: function( record, cb ){

    var self = this;

    var rnd = Math.floor(Math.random()*100 );
    consolelog( "\n");
    consolelog( rnd, "ENTRY: _updateCacheDelete ",  self.table, ' => ', record );

    self._updateParentsRecords( record, { upperCase: false, field: '_children', ifAutoload: true }, function( err ){
      if( err ) return cb( err );

      self._updateParentsRecords( record, { upperCase: true, field: '_searchData', ifAutoload: false }, function( err ){
        if( err ) return cb( err );
          
        consolelog( rnd, "EXIT: End of function." );

        cb( null );
      });
    });
  },



  _updateCacheInsert: function( record, cb ){

    var self = this;

    var rnd = Math.floor(Math.random()*100 );
    consolelog( "\n");
    consolelog( rnd, "ENTRY: _updateCacheInsert ",  self.table, ' => ', record );

    self._updateParentsRecords( record, { upperCase: false, field: '_children', ifAutoload: true, op: 'insert' }, function( err ){
      if( err ) return cb( err );

      self._updateSelfWithLookups( record, { upperCase: false, field: '_children', ifAutoload: true }, function( err ){
        if( err ) return cb( err );

      //  self._updateParentsRecords( record, { upperCase: true, field: '_searchData', ifAutoload: false, op: 'insert' }, function( err ){
      //    if( err ) return cb( err );

      //    self._updateSelfWithLookups( record, { upperCase: true, field: '_searchData', ifAutoload: false }, function( err ){
      //      if( err ) return cb( err );
      

        consolelog( rnd, "EXIT: End of function." );
        cb( null );

      //    });
      //  });

      });
    });

  },

  // Alias to updateCacheInsert
  _updateCacheUpdate: function( record, cb ){

    var self = this;

    var rnd = Math.floor(Math.random()*100 );
    consolelog( "\n");
    consolelog( rnd, "ENTRY: _updateCacheUpdate ",  self.table, ' => ', record );

    self._updateParentsRecords( record, { upperCase: false, field: '_children', ifAutoload: true, op: 'update' }, function( err ){
      if( err ) return cb( err );

      self._updateSelfWithLookups( record, { upperCase: false, field: '_children', ifAutoload: true }, function( err ){
        if( err ) return cb( err );

      //  self._updateParentsRecords( record, { upperCase: true, field: '_searchData', ifAutoload: false, op: 'insert' }, function( err ){
      //    if( err ) return cb( err );

      //    self._updateSelfWithLookups( record, { upperCase: true, field: '_searchData', ifAutoload: false }, function( err ){
      //      if( err ) return cb( err );


        consolelog( rnd, "EXIT: End of function." );
        cb( null );

      //    });
      //  });

      });
    });



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
    //consolelog("FIND IN SELECT: ",  mongoParameters.querySelector, self._projectionHash );

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


      // Make sure that lookup fields are looked up, and that
      // parent records containing this record also have their
      // cache updated
      self._updateCacheUpdate( record, function( err ){
        if( err ) return cb( err );

        // If options.multi is off, then use findAndModify which will accept sort
        if( !options.multi ){
          self.collection.findAndModify( mongoParameters.querySelector, mongoParameters.sortHash, { $set: recordToBeWritten, $unset: unsetObject }, function( err, doc ){
            if( err ) return cb( err );

            if( doc ){
              cb( null, 1 );
            } else {
              cb( null, 0 );
            }
          });

        // If options.multi is on, then "sorting" doesn't make sense, it will just use mongo's "update"
        } else {

          // Run the query
          self.collection.update( mongoParameters.querySelector, { $set: recordToBeWritten, $unset: unsetObject }, { multi: true }, cb );
        }
      });
    });
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

      // Make searchdata for the record itself, since every search will be based on
      // _searchData. It will copy all fields, regadrless of type, to make searching easier
      // (although only 'string' types will be uppercased).
      // TODO: re-establish this so that it writes the right fields, the ones without a "." in them
      /*recordToBeWritten._searchData = {};
      for( var k in recordToBeWritten ){
        if( k !== '_searchData' && k !== '_children' && typeof( recordToBeWritten[ k ] ) !== 'object' ){
          recordToBeWritten._searchData[ k ] = recordToBeWritten[ k ];
          if( typeof( recordToBeWritten[ k ] ) === 'string' ){
            recordToBeWritten._searchData[ k ] = recordToBeWritten._searchData[ k ].toUpperCase();
          }
        }
      }*/

      // Actually run the insert
      self.collection.insert( recordToBeWritten, function( err ){
        if( err ) return cb( err );

        // Make sure that lookup fields are looked up, and that
        // parent records containing this record also have their
        // cache updated
        self._updateCacheInsert( record, function( err ){
          if( err ) return cb( err );

          if( ! options.returnRecord ) return cb( null );

          self.collection.findOne( { _id: recordToBeWritten._id }, self._projectionHash, function( err, doc ){
            if( err ) return cb( err );

            if( doc !== null && typeof( self._fieldsHash._id ) === 'undefined' ) delete doc._id;

            cb( null, doc );
          });
        });
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
        if( err ) return cb( err );

        if( doc ){

          self._updateCacheDelete( doc , function( err ){
            if( err ) return cb( err );
            cb( null, 1 );
          });

        } else {
          cb( null, 0 );
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

    //consolelog("REPOSITIONING BASING IT ON ", positionField, "IDPROPERTY: ", idProperty, "ID: ", id, "TO GO AFTER:", moveBeforeId );

    // Case #1: Change moveBeforeId
    var sortParams = { };
    sortParams[ positionField ] = 1;
    self.select( { sort: sortParams, conditions: conditionsHash }, { skipHardLimitOnQueries: true }, function( err, data ){
      if( err ) return cb( err );
      //consolelog("DATA BEFORE: ", data );

      var from, to;
      data.forEach( function( a, i ){ if( a[ idProperty ].toString() == id.toString() ) from = i; } );
      //consolelog("MOVE BEFORE ID: ", moveBeforeId, typeof( moveBeforeId )  );
      if( typeof( moveBeforeId ) === 'undefined' || moveBeforeId === null ){
        to = data.length;
        //consolelog( "LENGTH OF DATA: " , data.length );
      } else {
        //consolelog("MOVE BEFORE ID WAS PASSED, LOOKING FOR ITEM BY HAND...");
        data.forEach( function( a, i ){ if( a[ idProperty ].toString() == moveBeforeId.toString() ) to = i; } );
      }

      //consolelog("from: ", from, ", to: ", to );

      if( typeof( from ) !== 'undefined' && typeof( to ) !== 'undefined' ){
        //consolelog("SWAPPINGGGGGGGGGGGGGG...");

        if( to > from ) to --;
        moveElement( data, from, to);
      }

      //consolelog("DATA AFTER: ", data );

      // Actually change the values on the DB so that they have the right order
      var item;
      for( var i = 0, l = data.length; i < l; i ++ ){
        item = data[ i ];

        updateTo = {};
        updateTo[ positionField ] = i + 100;
        //consolelog("UPDATING...");
        self.update( { conditions: { and: [ { field: idProperty, type: 'eq', value: item[ idProperty ] } ] } }, updateTo, function(err,n){ /*consolelog("ERR: " , err,n ); */} );
        //consolelog( item.name, require('util').inspect( { conditions: { and: [ { field: idProperty, type: 'eq', value: item[ idProperty ] } ] } }, updateTo , function(){} ) );
        //consolelog( updateTo );
      };

      cb( null );        
    });     

  },

  makeIndex: function( keys, options ){
    //consolelog("MONGODB: Called makeIndex in collection ", this.table, ". Keys: ", keys );
    var opt = {};

    if( typeof( options ) === 'undefined' || options === null ) options = {};
    opt.background = !!options.background;
    opt.unique = !!options.unique;
    if( typeof( options.name ) === 'string' )  opt.name = options.name;

    this.collection.ensureIndex( keys, opt, function(){} );
  },

  dropAllIndexes: function( done ){
    //consolelog("MONGODB: Called makeIndex in collection ", this.table, ". Keys: ", keys );
    var opt = {};

    this.collection.dropAllIndexes( done );
  },






  // **** DELETED FUNCTIONS ***

















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


