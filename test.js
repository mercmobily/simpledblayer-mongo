
/*
Copyright (C) 2013 Tony Mobily

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

// TODO: Run tests on indexes

var 
  dummy

, declare = require('simpledeclare')
, SimpleSchema = require('simpleschema')
, SimpleSchemaMongo = require('simpleschema-mongo')

, MongoMixin = require('./MongoMixin.js')

, mw = require('mongowrapper')
;


var SchemaMixin = declare( [ SimpleSchema, SimpleSchemaMongo ] );

var commonSchema = new SchemaMixin( {
  name    : { type: 'string', searchable: true, sortable: true },
  surname : { type: 'string', searchable: true, sortable: true },
  age     : { type: 'number', searchable: true, sortable: true },
  _id     : { type: 'id', searchable: true },
});


var simpledblayerTests = require( "./lib/simpledblayer/test.js" );

var tests = simpledblayerTests.get(

  function getDbInfo( done ) {
    mw.connect('mongodb://localhost/tests', {}, function( err, db ){
      if( err ){
        throw new Error("MongoDB connect: could not connect to database");
      } else {
        done( null, db, SchemaMixin, MongoMixin );
      }
    });
  },


  function closeDb( db, done ) {
    db.close( done );
  },

  function makeExtraTests( g ){

    return {

      "mongo prep": function( test ){

        g.mongoPeople = new g.Layer( 'mongoPeople', {  schema: commonSchema, idProperty: 'name' } );
        test.ok( g.mongoPeople );
        test.done();
      },

      "mongo adds _id field": function( test ){

         var person = { name: "Joe", surname: "Mitchell", age: 48 };
         g.mongoPeople.insert( person, { returnRecord: true }, function( err, personReturned ){
           test.ifError( err );
           test.ok( personReturned._id );
           test.notDeepEqual( person, personReturned, "Records match, but the returned one should have _id set" );
           test.done();
         });
      },

      "mongo and updating _id": function( test ){

         var person = { name: "Tory", surname: "Amos", age: 56 };
         g.mongoPeople.insert( person, function( err, personReturned ){
           test.ifError( err );

           g.mongoPeople.update( { conditions: { and: [ { field: 'name', type: 'is', value: 'Tory' } ] } }, { surname: "Me"  }, function( err, howMany ){
             test.equal( howMany, 1 );
           
             g.mongoPeople.makeId( null, function( err, id ){
               test.equal( typeof( id ), 'object' );

               g.mongoPeople.update( { conditions: { and: [ { field: 'name', type: 'is', value: 'Tory' } ] } }, { _id: id  }, function( err, howMany ){ 
                 test.equal( typeof( err ), 'object' ); 

                 test.done();

               });
             });
           });
         });
      },

      
      "indexing": function( test ){
        g.mongoPeople.generateSchemaIndexes( {}, function( err ){
          test.done();
        });
      }

    }


   

  }

);


for(var test in tests) {
    exports[ test ] = tests[ test ];
}



