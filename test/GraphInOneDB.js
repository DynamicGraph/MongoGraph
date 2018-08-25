// test_AtomicMDSimulation.js 
const MG = require("../MongoGraph.js") 
const assert = require('chai').assert;
//const AG = require('../AGraph');
const MongoClient = require('mongodb').MongoClient

describe('test a Graph in one DataBase', function(){ 
	console.log("Tables:'entities','edges'");
	console.log(" 1--->2");
    console.log(" |     ");
    console.log(" +--->3");
	let dbname = "MoonHo"
	var _v1=null, _v2=null, _v3=null
	it('Test create three vertices: "1", "2", "3" ', async() => { 
		try{
			let client = await MongoClient.connect('mongodb://localhost:27017', { useNewUrlParser: true});
			let gdb = new MG.Graph(client,{print_out:true});
			let startTime = gdb.begin_profiling("Main"); 
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
			gdb.end_profiling(startTime); 
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
			let startTime = gdb.begin_profiling("Main");  
				let edge12={_src:{table:"entities", _id:_v1._id}, _dst:{table:"entities", _id:_v2._id}};
				results = await gdb.insertEdge(dbname, "edges", edge12); 
				assert(results.ops.length==1);
				let edge13={_src:{table:"entities", _id:_v1._id}, _dst:{table:"entities", _id:_v3._id}};
				results = await gdb.insertEdge(dbname, "edges", edge13); 
				assert(results.ops.length==1); 
				//debugger;
				results = await gdb.getOutEV(dbname, "entities", {_id: _v1._id}, "edges", {});
				assert(results.length==5); // itself, two edges, two out vtxs
		    	await client.close();
			gdb.end_profiling(startTime); 
		}
		catch(err){
			console.log(err);
			assert(0);
		}  
	});
	 it('Test edges ', async() => { 
		try{
			 let client = await MongoClient.connect('mongodb://localhost:27017', { useNewUrlParser: true});
			let gdb = new MG.Graph(client,{print_out:true});
			let startTime = gdb.begin_profiling("Main"); 
				let results = await gdb.getOutEV(dbname, "entities", {_id: _v1._id}, "edges", {});
				assert(results.length==5); // itself, two edges, two out vtxs
		    	 
		    	results = await gdb.getInEV(dbname, "entities", {_id: _v1._id}, "edges", {});
				assert(results.length==1); // only itself

				results = await gdb.getInEV(dbname, "entities", {_id: _v2._id}, "edges", {});
				assert(results.length==3); // itself, one edge, one vtx

				results = await gdb.getInEV(dbname, "entities", {_id: _v3._id}, "edges", {});
				assert(results.length==3); // itself, one edge, one vtx

		    	await client.close();
			gdb.end_profiling(startTime); 
		}
		catch(err){
			console.log(err);
			assert(0);
		}  
	});

/*	it('check after first internal transition', function() {
		engine.make_run(true); // true is synchronous.
		let q = m.q_state_JSON();
		console.log("q1="+JSON.stringify(q));
		assert(q.tL==100);
		assert(q.tN==150);
		assert(q.state.phase==1);
	}) 
	it('check after 2nd internal transition', function() {
		engine.make_run(true); // true is synchronous.
		let q = m.q_state_JSON();
		console.log("q2="+JSON.stringify(q));
		assert(q.tL==150);
		assert(q.tN==250);
		assert(q.state.phase==0);
	}) */
	
});