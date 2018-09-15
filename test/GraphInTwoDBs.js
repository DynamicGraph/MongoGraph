// test_AtomicMDSimulation.js 
const MG = require("../MongoGraph.js") 
const assert = require('chai').assert;
//const AG = require('../AGraph');
const MongoClient = require('mongodb').MongoClient ;

describe('test a Graph in TWO DataBases', function(){ 
	console.log("Tables:'entities','edges'");
	console.log(" {db:member, table:members, {name:'Moon'} ---> {db:book, table:book, title:'A Happy Life'}<--{db:books, table:publisher, name:'A_publication'}"); 
	
	let db_members="Test_members"
	let db_books = "Test_books";
	var _member=null, _book=null, _publisher=null;
	it('Test delete  a member and a book in two DBs  ', async() => { 
		try{
			let client = await MongoClient.connect('mongodb://localhost:27017', { useNewUrlParser: true});
			let gdb = new MG.Graph(client, {print_out:true});
			gdb.begin_profiling("Main"); 
			    await gdb.clearDB(db_books); // clear all test DB 
			    await gdb.clearDB(db_members); // clear all test DB   
		    	await client.close();  
		    	//assert(0);
			gdb.end_profiling();   
		}
		catch(err){
			console.log(err);
			assert(0); 
		} 
	});

	it('Test create a member and a book in two DBs  ', async() => { 
		try{
			let client = await MongoClient.connect('mongodb://localhost:27017', { useNewUrlParser: true});
			let gdb = new MG.Graph(client, {print_out:true});
			gdb.begin_profiling("Main");   
			    let results = await gdb.insert(db_members, "members", {name:"Moon"});
			    results = await gdb.get(db_members, "members", {});
				_member = results[0];

				results = await gdb.insert(db_books, "books", {title:"A Happy Life"});
				results = await gdb.get(db_books, "books", {}); 
				_book = results[0];

				results = await gdb.insert(db_books, "publishers", {name:"A_publication"});
				results = await gdb.get(db_books, "publishers", {}); 
				_publisher = results[0];

		    	await client.close();  
		    	//assert(0);
			gdb.end_profiling();   
		}
		catch(err){
			console.log(err);
			assert(0); 
		} 
	});
	it('Test create the edge from a member to the book', async() => { 
		try{
			let client = await MongoClient.connect('mongodb://localhost:27017', { useNewUrlParser: true});
			let gdb = new MG.Graph(client,{print_out:true});
			gdb.begin_profiling("Main");  
				let edge={_src:{db:db_members, table:"members", _id:_member._id}, 
				          _dst:{db:db_books, table:"books", _id:_book._id}};
				results = await gdb.insertEdge(db_members, "members2books", edge);
				assert(results.ops.length==1); 
				edge={_src:{db:db_books, table:"publishers", _id:_publisher._id}, 
				      _dst:{db:db_books, table:"books", _id:_book._id}};
				results = await gdb.insertEdge(db_books, "publishers2books", edge);
				assert(results.ops.length==1); 

		    	await client.close();
			gdb.end_profiling(); 
		}
		catch(err){
			console.log(err);
			assert(0);
		}  
	});
	it('Test edge retrival', async() => { 
		try{
			 let client = await MongoClient.connect('mongodb://localhost:27017', { useNewUrlParser: true});
			let gdb = new MG.Graph(client,{print_out:true});
			gdb.begin_profiling("Main"); 
				let results = await gdb.getOutEV(db_members, "members", {_id: _member._id}, [{db:db_members, table:"members2books"}]);
				assert(results.length==3); // itself, one edge, one book
		    	//debugger; // 
		    	results = await gdb.getInEV(db_books, "books", {_id: _book._id}, 
		    							[{db: db_members, table:"members2books", condition:{}}, {db: db_books, table:"publishers2books"}]);
				assert(results.length==5); // itself, one edge, one member
		    	await client.close();
			gdb.end_profiling(); 
		}
		catch(err){
			console.log(err);
			assert(0);
		}  
	});
});