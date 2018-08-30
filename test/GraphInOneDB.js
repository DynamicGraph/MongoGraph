// test_AtomicMDSimulation.js 
const MG = require("../MongoGraph.js") 
const assert = require('chai').assert;
//const AG = require('../AGraph');
const MongoClient = require('mongodb').MongoClient ;

describe('test a Graph in one DataBase', function(){ 
	console.log("Tables:'entities','edges'");
	console.log(" 1--->2");
    console.log(" |     ");
    console.log(" +--->3");
	let dbname = "MoonHo";
	var _v1=null, _v2=null, _v3=null
	it('Test create three vertices: "1", "2", "3" ', async() => { 
		try{
			let client = await MongoClient.connect('mongodb://localhost:27017', { useNewUrlParser: true});
			let gdb = new MG.Graph(client,{print_out:true});
			gdb.begin_profiling("Main"); 
			    await gdb.clearDB(dbname); // clear all test DB 
				let results = await gdb.insert(dbname, "entities", [{id:1}, {id:2}, {id:3}])  
				let things = await gdb.get(dbname, "entities", {});   
				assert(things.length == 3);
				results = await gdb.get(dbname, "entities", {id:1})  
				assert(results.length == 1);
				_v1 = results[0];
				assert(_v1.id == 1);  
				results = await gdb.get(dbname, "entities", {id:2})  
				assert(results.length == 1);
				_v2 = results[0];
				assert(_v2.id == 2);  
				results = await gdb.get(dbname, "entities", {id:3})  
				assert(results.length == 1);
				_v3 = results[0];
				assert(_v3.id == 3);  
				let lastOne = await gdb.getLastOne(dbname, "entities", {});   
				assert(lastOne.id == 3)  
		    	await client.close();  
		    	//assert(0);
			gdb.end_profiling();   
		}
		catch(err){
			console.log(err);
			assert(0); 
		} 
	});
	it('Test create two edges: "1->2", "1->3" ', async() => { 
		try{
			let client = await MongoClient.connect('mongodb://localhost:27017', { useNewUrlParser: true});
			let gdb = new MG.Graph(client,{print_out:true});
			gdb.begin_profiling("Main");  
				let edge12={_src:{db:dbname, table:"entities", _id:_v1._id}, _dst:{db:dbname, table:"entities", _id:_v2._id}};
				results = await gdb.insertEdge(dbname, "edges", edge12); 
				assert(results.ops.length==1);
				let edge13={_src:{db:dbname, table:"entities", _id:_v1._id}, _dst:{db:dbname, table:"entities", _id:_v3._id}};
				results = await gdb.insertEdge(dbname, "edges", edge13); 
				assert(results.ops.length==1); 
		    	await client.close();
			gdb.end_profiling(); 
		}
		catch(err){
			console.log(err);
			assert(0);
		}  
	});
	it('Test edges retrival', async() => { 
		try{
			let client = await MongoClient.connect('mongodb://localhost:27017', { useNewUrlParser: true});
			let gdb = new MG.Graph(client,{print_out:true});
			gdb.begin_profiling("Main"); 
				let results = await gdb.getOutEV(dbname, "entities", {_id: _v1._id}, [{db:dbname, table:"edges", condition:{}}]);
				assert(results.length==5); // itself, two edges, two out vtxs
		    	 
		    	results = await gdb.getInEV(dbname, "entities", {_id: _v1._id}, [{db:dbname, table:"edges", condition:{}}]);
				assert(results.length==1); // only itself

				results = await gdb.getInEV(dbname, "entities", {_id: _v2._id}, [{db:dbname, table:"edges", condition:{}}]);
				assert(results.length==3); // itself, one edge, one vtx

				results = await gdb.getInEV(dbname, "entities", {_id: _v3._id}, [{db:dbname, table:"edges", condition:{}}]);
				assert(results.length==3); // itself, one edge, one vtx

		    	await client.close();
			gdb.end_profiling(); 
		}
		catch(err){
			console.log(err);
			assert(0);
		}  
	});
    it('Test remove function', async() => { 
		try{
			let client = await MongoClient.connect('mongodb://localhost:27017', { useNewUrlParser: true});
			let gdb = new MG.Graph(client,{print_out:true});
			gdb.begin_profiling("Main"); 
				let results = await gdb.remove(dbname, "entities", {_id: _v1._id}, [{db:dbname, table:"edges"}]); 
		    	 
		    	results = await gdb.get(dbname, "entities", {});
				assert(results.length==2); // v2 and v3 remains. 

				results = await gdb.get(dbname, "edges",  {});
				assert(results.length==0); // all edges are deleted 

				results = await gdb.getInEV(dbname, "entities", {_id: _v2._id}, [{db:dbname, table:"edges", condition:{}}]);
				assert(results.length==1); // only itself

		    	await client.close();
			gdb.end_profiling(); 
			//assert(0); // intentional fail for checking hanging. 
		}
		catch(err){
			console.log(err);
			assert(0);
		}  
	}); 
	it('Test update function', async() => { 
		try{
			let client = await MongoClient.connect('mongodb://localhost:27017', { useNewUrlParser: true});
			let gdb = new MG.Graph(client,{print_out:true});
			gdb.begin_profiling("Main"); 
				let result = await gdb.update(dbname, "entities", {_id: _v2._id}, {id:22} ); 
				assert(result.modifiedCount==1); //   
				let results = await gdb.get(dbname, "entities", {_id: _v2._id}); 
				assert(results[0].id == 22);
		    	await client.close();
			gdb.end_profiling(); 
			//assert(0); // intentional fail for checking hanging. 
		}
		catch(err){
			console.log(err);
			assert(0);
		}  
	}); 
});