//event is a JSON file with details regarding the data to be collected from firebase.
//Sample input JSON:
/*
{
request:{
	path="PATH", #where payload should be stored/retrieved from the database.
	payload=null/.JSON #null if read request, .JSON file if write request. .JSON file if update requested should only
	contain key-value pairs of items to be updated.
	
	N.B.!! If any dates are being written, which may be filtered in future, be sure to also include an auxilliary key 
	'U'+ datekeyname, with its value being universal time, milliseconds since 1/1/1970 00:00. Eg. if adding an order to db
	with OrderDate=1/1/1971, must also include another key value pair .UOrderDate=31536000
	
	
	//QUERY
	key=Which child key to order the data by.
	startAt=starting value
	endAt=ending value (lexicographical)
	
}
details:{
	type:"Read"/"Write"/"Update"/"Query"
	//if query
	querytype:"betweendates"/"between"/"equaldate"
}
}	
*/


exports.handler=(event,context)=>{
	var firebase=require('firebase/app');
	require('firebase/auth');
	require('firebase/database');
// Initialize Firebase
    var config = {
      apiKey: "AIzaSyBPK4x0AGLbL9aVeUMkT4XT54UXN3gkQZk",
      authDomain: "arnottsbt-db.firebaseapp.com",
      databaseURL: "https://arnottsbt-db.firebaseio.com",
      projectId: "arnottsbt-db",
      storageBucket: "arnottsbt-db.appspot.com",
      messagingSenderId: "1041466005153"
    };
	
	if(!firebase.apps.length){
    	firebase.initializeApp(config);
	}
	try{
	//switch function to perform the requested operation
		switch(event.details.type){
			
			case "Read":
				readData(event.request.path);
				break;
			case "Write":
				if(event.details.override) writeData(event.request.path,event.request.payload)
				else writeDataWithTest(event.request.path,event.request.key,event.request.payload,event.request.time);
				break;
			/* Not required for now.
			case "Update":
				updateData(event.request.path,event.request.payload);
				
				break;*/
			case "Query":
				queryDB(event);
				break;
			default:
				throw {"message":"details.type must either be 'Read', 'Write' or 'Update'."};
		}
	}
	catch(err){
		context.succeed({
			"status":"error",
			"errormessage":err.message,
			"errorstack":err.stack
		}
		);
	}
	//checks if a similar order exists in the previous time days.(Similar if it has the same brand, criteria and department)
	//If not, writes order to the database, otherwise returns a caution message with details of the orders that already exist.
	async function writeDataWithTest(path,key,payload,time){
		var criteria=BCDcriteria(payload);
		var ref=firebase.database().ref(path);
		var result=[];
		
		enddate=new Date();
		endstring=enddate.toISOString();
	
		//subtracting appropriate number of milleseconds off current time, then converting back to ISO-String format
		startdate=new Date(enddate.getTime()-1000*60*60*24*time);
		startstring=startdate.toISOString()
		
		//checking the past time days for orders satisfying criteria
		ref.orderByChild(key).startAt(startstring).endAt(endstring)
		.on('value',function(snapshot){
			orderexists=false;
			for(var orderno in snapshot.val()){
				
				order=snapshot.val()[orderno];
				if(meets_criteria(order,criteria)){
					orderexists=true;
					result.push(order);
				}
			}
			if(orderexists) context.succeed({"status":"caution","orders":result});
			else writeData(path,payload);		
		});
		
	}
	//writes the given data to the path specified in the NoSQL db in Firebase,returning a simple JSON file 
	//with success/failure details.
	async function writeData(path,jsonstring){
		json=JSON.parse(jsonstring);
		await firebase.database().ref(path+"/"+json.OrderNo)
		.set(json)
		.then(function(){
			context.succeed({"status":"success","errordetails":null});
		})
		.catch(function(error){
			context.succeed({"status":"error","errordetails":error});
		});
	}
	//returns the data at the path specified in the NoSQL db in Firebase, in JSON format.
	function readData(path){
		
		firebase.database().ref(path)
		.on('value',function(snapshot){

			context.succeed(snapshot.val());
		})
	}
	
	//updates the key/value pairs to what the given json specifies, leaving other key/value pairs  untouched.
	//NOT TESTED!!
	async function updateData(path,json){
		await firebase.database().ref(path).update(json)
		.then(function(){context.succeed({"status":"success","errordetails":null})}).
		catch(function(error){context.succeed({"status":"error","errordetails":error})});
	}
	
	function queryDB(json){
		
		switch(json.details.querytype){
			case "between":
				queryBetween(json.request.path,json.request.key,json.request.startAt,json.request.endAt,json.request.criteria);
				break;
			case "equaldate":
				queryEqualDate(json.request.path,json.request.key,json.request.equal,json.request.criteria);
				break;
			case "betweenboolean":
				queryBoolean(json.request.path,json.request.key,json.request.criteria,json.request.time);
				break;
			default:
				throw {"message":"Invalid Query type"};
			
		}
	}
	//date argument is an ISO-string, with no time details. Should be string of form "YYYY-MM-DD"
	//checks a certain day to see if any of the orders meet the criteria.
	function queryEqualDate(path,key,date,criteria){
		if(!(date==null || date=="")) {
			startAt=date+"T00:00:00";
			endAt=date+" 23:59:59";
			queryBetween(path,key,startAt,endAt,criteria)
		}
		else throw {"message":"Date cannot be null"};
	}
	/*Criteria is a list of requirements that the output must have.
	{Brand:["Ralph-Lauren","Tommy-Hilfiger"],Category:["Shirts"]} means only entries which have the key value pair Brand:"Ralph-Lauren" or "Tommy-Hilfiger"   and
	the key value pair Category:"Shirts" will be included*/
	
	/*time is in days. criteria is explained above. Checks if any orders in the database in the past time days meets the
	criteria given. Returns an object with status report, boolean indicating whether such an order exists, and an
	array of all orders satisfying the criteria.*/
	
	function queryBoolean(path,key,criteria,time){
		var ref=firebase.database().ref(path);
		var result=[];
		
		enddate=new Date();
		endstring=enddate.toISOString();
	
		//subtracting appropriate number of milleseconds off current time, then converting back to ISO-String format
		startdate=new Date(enddate.getTime()-1000*60*60*24*time);
		startstring=startdate.toISOString()
		ref.orderByChild(key).startAt(startstring).endAt(endstring)
		
		.on('value',function(snapshot){
			orderexists=false;
			for(var orderno in snapshot.val()){
				
				order=snapshot.val()[orderno];
				if(meets_criteria(order,criteria)){
					orderexists=true;
					result.push(order);
				}
			}
			context.succeed({"status":"success","orderexists":orderexists,"orders":result});
				
		});
	}
	
	function meets_criteria(order,criteria){
		for(var crit in criteria){
			if(!criteria[crit].includes(order[crit])) return false;
		}		
		return true;
	}
	
	//Dates must be ISO string formatted (YYYY-MM-DDTHH:MM:SS)
	function queryBetween(path,key,startAt,endAt,criteria){
		var result=[];
		var ref=firebase.database().ref(path);
		ref.orderByChild(key).startAt(startAt).endAt(endAt)
		.on('value',function(snapshot){
			for(var orderno in snapshot.val()){
				order=snapshot.val()[orderno];
				if(meets_criteria(order,criteria)){
					result.push(order);
				}
			}
			context.succeed(result);
		});
	}
	//criteria object maker for write with test.
	function BCDcriteria(jsonstring){
		json=JSON.parse(jsonstring);
		criteria={};
		criteria['Brand']=[json.Brand];
		criteria['Class']=[json.Class];
		criteria['Department']=[json.Department];
		return criteria;
	}
}
//Testing (need to replace context.succeed with a callback function if testing in node console)
//exports.handler({"request":{"time":100,"key":"OrderDate","path":"/Orders","payload":"{\r\n\"OrderNo\":40001,\"Brand\":\"7 Jewellery\",\"Class\":\"Accessories\",\"Department\":\"Charlotte Tilbury\"}"
//},"details":{"type":"Write","override":false}},
//function(string){console.log(string);});
//.handler({"request":{"path":"/"},"details":{"type":"Read"}},function(string){console.log(string);});