/**
  Author: Moon Ho Hwang
  email: {moon.hwang}@gmail.com
  
  Modification History 
     Jan/25/2017: create
*/
var MongoClient = require('mongodb').MongoClient
  , assert = require('assert');
 
var MG = MG || {version:"1.0", ky_db:"_db", ky_id:"_id", ky_cl:"_collection", ky_src:"_src", ky_dst:"_dst"} 

    
/**
 * MG.Graph constructor 
 * @constructor
 */
MG.Graph = function(client, options){
	this.client = client,  
	this.print_out = (options && options.print_out)?options.print_out:false,
	this.fname_stack=[],

	
    /**
     * Insert a set of documents at a collection of a DB
     * @param {string} dbName - Mongo DB name
     * @param {string} collection_name - the name of collection. 
     * @param {Object|Object[]} Docs - a single document, or an array of documents.  
     * @return {} results. 
    */
	this.insert = async function(dbName, collection_name, Docs) { 
		let results=null;
		try {
			this.begin_profiling("insert"); 
			const db = this.client.db(dbName);
			let collection = await db.collection(collection_name); // this.check_getCollection(db, collection_name, Docs); 
			
			if(Array.isArray(Docs))
			    results= await collection.insertMany(Docs);
			else if (Docs instanceof Object)
				results= await collection.insertOne(Docs);   
		}
		catch(err){
        	console.log(err)
        }
        finally{
        	this.end_profiling(results); 
        	return results;
        } 
	}
	
	
    /**
     * Get a set of documents at a given collection.  
     * @param {string} dbName - Mongo DB name
     * @param {string} collection_name - the name of collection. 
     * @param {Query} condition - a query of MongoDB find() command.  
     * @return {Array} results. 
    */
	this.get = async function(dbName, collection_name, condition) {  
		let results=null;
		try {
			this.begin_profiling("get");
			let db = this.client.db(dbName);
			results = await db.collection(collection_name).find(condition).toArray();
		}
		catch(err){
        	console.log(err)
        }
        finally{
        	this.end_profiling(results); 
        	return results;
        } 
	}  
    
	/**
     * Get the last documents of  a given collection.  
     * @param {string} dbName - Mongo DB name
     * @param {string} collection_name - the name of collection. 
     * @param {Query} condition - a query of MongoDB find() command.  
     * @return {Array} results. 
    */
	this.getLastOne = async function(dbName, collection_name, condition) { 
		let results = null 
		try {
			this.begin_profiling("getLastOne");
			let db = await this.client.db(dbName);
			let collection = await db.collection(collection_name);  
			let cursor = await collection.find(condition).sort({_id:-1}).limit(1); 
			//console.log(cursor);
	        if(cursor) {
	            let items = await cursor.toArray();
	            if(items && items.length>0)  
	            	results=items[0];
	        }
   		}
		catch(err){
        	console.log(err)
        }
        finally{
        	this.end_profiling(results); 
        	return results;
        } 
	}
	 
	/**
     * Get a set of destinations, their incoming edges, and their sources. 
     * @param {string} dbName - Mongo DB name
     * @param {string} dst_collection_name - the name of collection for a given targets.
     * @param {Query} dst_condition - a query of MongoDB find() command to defined a set of targets.
     * @param {string} edge_col_name - the name of collection for the incoming edges to the targets.
     * @param {Query} edge_condition - a query of MongoDB find() command to defined the incoming edges to the targets. 
     * @return {Array} contating all destiations, incoming edges, and incoming sources. 
    */
	this.getInEV = async function(dbName, dst_collection_name, dst_condition, edge_col_name, edge_condition) { 
		let resultArray=[]; // storage for finding. 
		try {
			this.begin_profiling("getInEV"); 
			let db = await this.client.db(dbName);
			let dst_collection = await db.collection(dst_collection_name); // this.check_getCollection(db, dst_collection_name, dst_condition);  
			let edge_col = await db.collection(edge_col_name); //this.check_getCollection(db, edge_col_name, edge_condition);  
			
			let IdSet = {}; // to make sure unique elements.     
			let Ditems = await dst_collection.find(dst_condition).toArray(); 
			for(let ii=0; ii<Ditems.length; ii++) {
				let dst = Ditems[ii];  
				if(!(dst._id in IdSet)) {  IdSet[dst._id]=dst;
	                resultArray.push(dst); // add each destination
				}
				//console.log("given vtx="+JSON.stringify(dst)); 
				let ext_edge_condition = JSON.parse(JSON.stringify(edge_condition)); // copy 
				ext_edge_condition["_dst._id"]=dst._id; // add destination condition 
					
				let Eitems = await edge_col.find(ext_edge_condition).toArray();
				for(let jj=0; jj < Eitems.length; jj++) {
					let edge = Eitems[jj];
					if(!(edge._id in IdSet)) { IdSet[edge._id]=edge;
						resultArray.push(edge); // add each incoming edge 
					} 
					//console.log("edge="+JSON.stringify(edge));
					let src_col = await db.collection(edge._src.table); 
					let Sitems= await src_col.find({_id:edge._src._id}).toArray();
					// assume that the source exists for each edge   
					    
					for(let kk=0; kk < Sitems.length; kk++) {
						let src = Sitems[kk];
						if(!(src._id in IdSet)){ IdSet[src._id]=src;
							resultArray.push(src);// add each source of an edge 
						} 
					}  // for source 
				} // for each incoming edge 
			}// for each destination  
		}
		catch(err){
        	console.log(err)
        }
        finally{
			this.end_profiling(resultArray);
			return resultArray;
		}
	} 
    
    /**
     * Get a set of sources, their outgoing edges, and their destinations. 
     * @param {string} dbName - Mongo DB name
     * @param {string} src_collection_name - the name of collection for a given sources.
     * @param {Query} src_condition - a query of MongoDB find() command to defined a set of sources.
     * @param {string} edge_col_name - the name of collection for the outgoing edges to the sources.
     * @param {Query} edge_condition - a query of MongoDB find() command to defined the outgoing edges to the sources.
     * @return {Array} contating all destiations, incoming edges, and incoming sources.
    */
	this.getOutEV = async function(dbName, src_collection_name, src_condition, edge_col_name, edge_condition) {  
		let resultArray=[]; // storage for finding.  
		try {
			this.begin_profiling("getOutEV"); 
			let db = await this.client.db(dbName);
			let src_collection = await db.collection(src_collection_name); //this.check_getCollection(db, src_collection_name, src_condition);  
			let edge_col = await db.collection(edge_col_name); //this.check_getCollection(db, edge_col_name, edge_condition);   
					
			
			let IdSet = {}; // to make sure unique elements.        

			let Sitems = await src_collection.find(src_condition).toArray(); 
			for(let ii=0; ii<Sitems.length; ii++) {
				let src = Sitems[ii];
				if(!(src._id in IdSet)) { IdSet[src._id]=src;
	            	resultArray.push(src); 
				} 
				//console.log("given vtx="+JSON.stringify(src)); 
				let ext_edge_condition = JSON.parse(JSON.stringify(edge_condition)); // copy 
				ext_edge_condition["_src._id"]=src._id; // add source condition 
					
				let Eitems = await edge_col.find(ext_edge_condition).toArray();
				for(let jj=0; jj < Eitems.length; jj++) {
					let edge = Eitems[jj]; 
					if(!(edge._id in IdSet)) { IdSet[edge._id]=edge;
				        resultArray.push(edge); 
					} 
					//console.log("edge="+JSON.stringify(edge));
					let dst_col = await db.collection(edge._dst.table); 
					let DItems= await dst_col.find({_id:edge._dst._id}).toArray();				  
					for(let kk=0; kk < DItems.length; kk++) {
						let dst = DItems[kk]; 
						if(!(dst._id in IdSet)) { IdSet[dst._id]=dst;
			                resultArray.push(dst); 
						}  
					} // for each destination of an edge 
				}// for each edge 
			} // for each source 
		}
		catch(err){
        	console.log(err)
        }
        finally{
			this.end_profiling(resultArray);
			return resultArray;
		}
	}  
	
	/**
     * remove a set of documents given as well as their connecting edges
     * @param {string} dbName - Mongo DB name
     * @param {string} collection_name - the name of collection for a given sources.
     * @param {Query} condition - a query of MongoDB find() command to defined a set of sources.
     * @param {string[]} edge_collection_names - an array of names of edge collections.
    */
	this.remove = async function(dbName, collection_name, condition, edge_collection_names) {   
		let deleteResults=[]; // array of deleting items 
		try {
			this.begin_profiling("remove"); 
	       
	      	let arrayRemoveTargets = await this.get(dbName, collection_name, condition);
	      	let db = await this.client.db(dbName); 
	        for(let vid=0; vid < arrayRemoveTargets.length; vid++) { // for each removing targets given  
	            let doc = arrayRemoveTargets[vid];
	            let edge_condition={ $or:[{ "_src._id" : doc._id}, { "_dst._id" : doc._id}] };  
	            console.log(edge_condition);
	            for(let ii=0; ii< edge_collection_names.length; ii++) { // for each edge collections 
	                let edge_col_name = edge_collection_names[ii];
	                let edge_col = await db.collection(edge_col_name); //that.check_getCollection(db, edge_col_name, {});
	                //-- delete connected edges to the vertex doc.
	                let results = await edge_col.deleteMany(edge_condition);  
	                deleteResults.concat(results); // collect deleting items  
	            } // for each candidate edge collections   
	            let vtx_col = await db.collection(collection_name); // that.check_getCollection(db, collection_name, condition);  
	            let results = await vtx_col.deleteOne(doc); 
	       		deleteResults.push(results); // results.ops); 
	        } // for each target
	    }
	    catch(err){
        	console.log(err)
        }
        finally{
	        this.end_profiling(deleteResults);
	        return deleteResults;
	    }
	} 
    
    /**
     * update a document
     * @param {string} dbName - Mongo DB name
     * @param {string} collection_name - the name of collection. 
     * @param {Query} condition - a query of MongoDB find() command.  
     * @param {Object} newdoc - a new document  
    */
	this.update = async function(dbName, collection_name, condition, newdoc) {  
		let result = null;
		try {
			this.begin_profiling("update"); 
			let db = await this.client.db(dbName);
			let collection = await db.collection(collection_name); //this.check_getCollection(db, collection_name, condition); 
			result = await collection.updateOne(condition, {$set: newdoc} );
		}
		catch(err){
        	console.log(err)
        }
        finally{
			this.end_profiling(result);
			return result;
		}
	} 

	/**
     * clear all collections of a given DB. 
     * @param {string} dbName - Mongo DB name 
    */
    this.clearDB = async function(dbName) {  
    	let results = null;
    	try {
    		this.begin_profiling("clearDB");
    		let db = await this.client.db(dbName); 
    		results = await db.dropDatabase();
    	}
        catch(err){
        	console.log(err)
        }
        finally{
        	this.end_profiling(results); 
        	return results;
        } 
    } 
	this.begin_profiling = function(fname) {
		let startTime = new Date(); 
		this.fname_stack.push({func_name:fname, startTime:startTime}); // store function name and time stamp
		
		if(this.print_out) {
			let tabstr = "";
			for(let ii=0; ii<this.fname_stack.length-1; ii++)
				tabstr +="\t";
			console.log(tabstr+"-> "+fname);
		} 
	}
	this.end_profiling = function(results) { 
		let startTime = this.fname_stack[this.fname_stack.length-1].startTime;  
		if(this.print_out) {
			let fname = this.fname_stack[this.fname_stack.length-1].func_name; 
			let tabstr = "";
		    for(let ii=0; ii<this.fname_stack.length-1; ii++)
			    tabstr +="\t";
			if(results)
				console.log(tabstr+"\t"+JSON.stringify(results));
			let elapsedt = (new Date() - startTime)/1000;
			console.log(tabstr+"<- " + fname+ ": " + elapsedt+ " secs"); 
		}
		this.fname_stack.pop();// pop front
	}

}    

if (typeof module != 'undefined') // node 
	module.exports = MG; //  
