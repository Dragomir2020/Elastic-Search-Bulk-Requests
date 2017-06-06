//////////////////////////////////////////////////
//Read:Parse:Write and index in ES (importES.js)//
//////////////////////////////////////////////////

////////////////
//Contributers//
////////////////

///////////////////////////////////////////////

// NAME                   DATE           CHANGE
// Dillon Dragomir        06-03-2017     Initalized folder
// Dillon Dragomir        06-04-2017     Fixed Functionality with sending bulk requests to elastic search

///////////////////////////////////////////////

////////////////
//DEPENDANCIES//
////////////////

// Require Dependancies
var elasticsearch = require('elasticsearch'),
 	fs = require('fs'),
	xlsxj = require("xlsx-to-json");

/////////////
//VARIABLES//
/////////////

var Date = "20190806",
	Upload_time = 4,
	Player,
	GameType = "Sample"
	DataType = "Example",
	fileName = Date + "_" + GameType + "_" + DataType;

var globalPlayerInt,
	idSum = 0;

//New json object needs created for each loop of the for loop

// use for loop to get all players

for(var loop = 1; loop < 23; loop++){

	// Need new object each time


	var updatedFileName;
	//Get Player number
	if(loop<10){
		Player = "0" + loop;
	} else {
		Player = loop;
	}

	//Correct fileName
	updatedFileName = Player + "_" + fileName;
	
	// CHECK FILEPATH
	// Make sure the file exists before running data parsing
	if (fs.existsSync("csv/" + updatedFileName + ".xlsx")) {
		  read_file(Player,updatedFileName);

	} else {
		console.log(updatedFileName + " does not exist.");
	}
}

// Done executing for loop index data into ES
afterRetrieval();

// Call function from inside code
function write_file(L,Up,JS) {
	// VERIFY FILEPATH
	fs.writeFile(("json/" + L + "-" + Up + ".json"), JSON.stringify(JS), function (err) {
	  if (err) {
		  return console.log(err);
	  } 
	});
}

function read_file(PlayerR,uFN){
	var json;
	//USES FILEPATH DECLARED ABOVE
	xlsxj({
			input: ("csv/" + uFN + ".xlsx"), 
			output: null //Overwrite this output
		  }, function(err, result) {
			if(err) {
			  console.error(err);
			}else {
			  json = result;
			  add_data(PlayerR, Upload_time, json);
			}

	  	});
}

function add_data(Pl,Ut,json_add) {
	// Loop through json and add needed parameters
	for(var i = 0; i < json_add.length; i++) {
		json_add[i].Date = Date;
		json_add[i].Player = Pl;
		json_add[i].GameType = GameType;
		json_add[i].DataType = DataType;
	}
	// Call write function now
	write_file(Pl,Ut,json_add)
}

// Wait for data to be read parsed and wrote before indexing into elastic search
function afterRetrieval(){
	// For loop for each player
	for(var i = 1; i < 23; i++){
		globalPlayerInt = (i-1);
		//Add zero to beggining if less than 1o
		var Player,
			pubs;
		if(i<10){
			Player = "0" + i;
		} else {
			Player = i;
		}
		//Check whether filepath is valid
		// USES FILEPATH AGAIN
		if (fs.existsSync("json/" + Player + "-" + Upload_time + ".json")) {
				// Read file into variable
				pubs = JSON.parse(fs.readFileSync("json/" + Player + "-" + Upload_time + ".json"));
				// Function call that sends data to ES
				sendES(pubs,Player);
		} else {
				console.log(Player + "_" +  fileName + " does not exist.");
		}

	}
}

// MAKES BULK REQUEST
// Function calls elastic search database
function sendES(pubs,Player){
	// Create new client
	// VERIFY CLIENT IS ON LOCALHOST:9200
	var client = new elasticsearch.Client({  // default is fine for me, change as you see fit
		host: 'localhost:9200'
		//log: 'trace'
	});
	
	var bulk_request = new Array(),
		jsonData;

	jsonData = JSON.stringify({"index":{"_index":"women_soccer","_type":"sample","_id":"1"}});

	
	// Create new array of JSON for bulk request
	for(var j = 0; j < pubs.length; j++){
		bulk_request[j*2] = JSON.parse(jsonData);
		bulk_request[j*2].index._index = "women_soccer";
		bulk_request[j*2].index._type = fileName;
		bulk_request[j*2].index._id = j + idSum;
		//Parse time
		if(pubs[j].Time.substr(9,10) == "PM"){
			// Add 12 to make military time
			pubs[j]['timestamp'] = pubs[j].Date + " " + (parseInt(pubs[j].Time.substr(0,1)) + 12) + pubs[j].Time.substr(2,7);
		} else {
			// Must be AM
			pubs[j]['timestamp'] = pubs[j].Date + " " + pubs[j].Time.substr(0,7);
		}
		//console.log(pubs[j]);
		bulk_request[j*2 + 1] = pubs[j];
		//bulk_request[j*2 + 1]._timestamp = pubs[j].Time;	
	}
	//Bulk request to elastic search index
	client.bulk({
		body: bulk_request
		}, function(error, response) {
			if (error) {
			  console.error(error);
				return;	
		}
		else {
			console.log("Player " + Player + " data ingested.");
		}
	});
	idSum = idSum + pubs.length;
	// Vary important to do or js heap will fill up
	// Delete object
	delete bulk_request
	//delete client;
	delete client;	
}

/*
   	// USE FOR REALTIME INDEXING OF DATA
    //Create ES client and upload json objects
    pubs = JSON.parse(fs.readFileSync('json' + '/app10.json')); // name of my first file to parse
    //forms = JSON.parse(fs.readFileSync(__dirname + '/forms.json')); // and the second set

    //console.log(pubs.length);
    //var rounds = Math.floor(pubs.length/30000);

    //for(var r = 0; r <= 2; r++) {
        //console.log(r);

        //var mover = 30000*rounds;
        
            var client = new elasticsearch.Client({  // default is fine for me, change as you see fit
                host: 'localhost:9200'
                //log: 'trace'
            });
      
        //send 50,000 documents at a time
        for (var i = 0; i < 30000 ; i++ ) {
            //if(pubs.length <= i+1){
            //    break; //break at end of calls
            //}
    
          client.create({
            index: "server_data_1.0.5", // name your index
            type: "app10", // describe the data thats getting created
            id: i,// incremet ID every iteration - I already sorted mine but not a requirement
            //timestamp: "2017-02-05'T'12:20:20",
            body: pubs[i] // *** THIS ASSUMES YOUR DATA FILE IS FORMATTED LIKE SO: [{prop: val, prop2: val2}, {prop:...}, {prop:...}] - I converted mine from a CSV so pubs[i] is the current object {prop:..., prop2:...}
          }, function(error, response) {
            if (error) {
              console.error(error);
              return;
            }
            else {
            //console.log("importing data!!");  I don't recommend this but I like having my console flooded with stuff.  It looks cool.  Like I'm compiling a kernel really fast.
            }
          });
        }
*/

/*
	USE FOR CSV
	
    var Converter = require("csvtojson").Converter;
    // create a new converter object
    var converter = new Converter({});

    // call the fromFile function which takes in the path to your 
    // csv file as well as a callback function
    var json;
    //FILEPATH FOR READ
    converter.fromFile("csv/01_20150805_Practice1_Summary.xlsx",function(err,result){
        // if an error has occured then handle it
        if(err){
            console.log("An Error Has Occured");
            console.log(err);  
        } 
        // create a variable called json and store
        // the result of the conversion
        json = result;
        
        //FILEPATH FOR WRITE
        fs.writeFile('json/01_20150805_Practice1_Summary.json', JSON.stringify(json), function (err) {
          if (err) return console.log(err);
          console.log('Saved');
        });
        // log our json to verify it has worked
    });

  
  	delete converter;
   */ 
