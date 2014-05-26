//
// worker.js: main file for working with long running processes
//

var request = require('request'),
    mongoose = require('mongoose'),
    PORT = process.env.PORT || 5001,
    NB_ACCESS_TOKEN = process.env.NB_ACCESS_TOKEN,
    baseUri = 'https://' + process.env.NB_SLUG + '.nationbuilder.com/',
    allPeopleUri =  baseUri + 'api/v1/' + 'people',
    allListsUri =  baseUri + 'api/v1/' + 'lists',
    allEventsUri = baseUri + 'api/v1/' + 'sites/' + process.env.NB_SLUG + '/pages/events',
    rsvpsForEventUri = baseUri + 'api/v1/' + 'sites/'
                       + process.env.NB_SLUG + '/pages/events',
    peopleInAListUri = baseUri + 'api/v1/' + 'lists';


var mongoUri = process.env.MONGOLAB_URI ||
               'mongodb://localhost/theGreensApp';

var conn = mongoose.connect(mongoUri, function (err, res) {
    if (err) throw new Error('ERROR: ' + err);
 
    console.log('SUCCESS: connected to mongoDB: ' + mongoUri);
});


var Jobs = new mongoose.Schema(
    {
        jobType: String,
        httpMethod: String,
        personId: Number,
        listId: Number
    },
    {
        capped: 8000000
    }
);


var JobsModel = conn.model('Job', Jobs);




function readAndWork () {
    console.log('readAndWork function');

    JobsModel.find({}, function (err, jobs) {
        if (err) throw new Error(err);

        console.log('GOT JOBS TODO...');
        console.log('first one is jobs[0]: ' + jobs[0]);

        //for now just use the first model in array
        var personId = jobs[0].personId,
            listId   = jobs[0].listId;

        getAllPeopleInAList(listId, function (err, results) {
            console.log('in getAllPeopleInAList callback');
            if (err) throw new Error(err);

            var i, people = results.people;


            for (i = 0; i < people.length; i++) {
 
                //typeof people[i].personId = number
                //typeof personId = string
                //=> must parse string to int
                if (people[i].personId === parseInt(personId, 10)) {
                    console.log('isInList = TRUE');
                    return setTimeout(readAndWork, 10000);
                }
            } 

            console.log('isInList = FALSE');
            return setTimeout(readAndWork, 10000);
        });

        //setTimeout(readAndWork, 5000);
    });
}


function getAllPeopleInAList(listId, cb) {
    var perPage = 100, //seems to be the sweet spot to avoid resp timeouts
        allPeopleArray = [], //holds all of the people in a list 
        totalPages,
        totalNumberOfPeople,
        extraUrls = [],
        accessToken = process.env.NB_ACCESS_TOKEN,
        firstPageOfPeople = peopleInAListUri + '/' + listId + '/people' +
    		      '?access_token=' + accessToken + 
    		      '&page=1&per_page=' + perPage,
        optionsForFirstRequest = {
    	url: firstPageOfPeople,
    	method: 'GET',
    	headers: {
    	    //'User-Agent': userAgent,
    	    'Content-Type': 'application/json',
    	    'Accept': 'application/json'
     	    }
        },
        reducedPeopleArray= []; //holds only person id, first and lastname
                                // which is sent back 

    
    function callbackForFirstRequest(error, response, body) {
        console.log('in callbackForFirstRequest for GET people in list  req.');
        
    
        if (error) return errorCb(error);
      
        if (response.statusCode == 200) {
    	var bodyObject = JSON.parse(body), // a string
    	    results = bodyObject.results;
    
    	totalNumberOfPeople = bodyObject.total;
    	totalPages = bodyObject.total_pages; // a number
    	 
    	console.log('totalNumberOfPeople: ' + totalNumberOfPeople);
    	console.log('totalPages: ' + totalPages);
    
    	//append individual first page people to peopleArray
    	for (var i = 0; i < results.length; i++) {
    	    allPeopleArray.push(results[i]);
    
    	    reducedPeopleArray.push({
    		personId  : results[i].id,
    		firstName : results[i].first_name,
    		lastName  : results[i].last_name
    	    });        
    	}
    
    	//see if we need to paginate to get all people 
    	if (totalPages === 1) {
    	    //DONT need to paginate
    	    
    	    //create and save all the events to mongodb
    	    //saveAllListsToMongo();
    
    	    return cb(null, {'people': reducedPeopleArray});
    
    	} else {
    	    //DO need to paginate
    	    console.log('With per_page= ' + perPage + ' => have ' 
    		      + (totalPages - 1) + ' to get.');
    
    	    //create all the extra urls we need to call
    	    for (var j = totalPages ; j > 1; j--) {
    		var aUrl = peopleInAListUri + '/' + listId + '/people' + 
                           '?access_token=' + accessToken +
    			   '&page=' + j + '&per_page=' + perPage;
    		extraUrls.push(aUrl);
    	    }
   
    	    //start the heavy lifting to get all the pages concurrently
    	    downloadAllAsync(extraUrls, successCb, errorCb);                
    	}
    
        } else {
    	    return errorCb(response.statusCode);
        }
    }
    
    
    function successCb(result) {
        console.log('successCb called. got all results');
        //
        //result is of structure:
        // result = [    {page:3, ..., results: [{person}, {person}, ..., {person}]},
        //             , {page:4, ..., results: [{person}, {person}, ..., {person}]}
        //             , ...
        //             , {page:8, ..., results: [{person}, {person}, ..., {person}]}
        //          ];
        //
    
        var i, j;
    
        //result is an array of arrays wih objects
        for (i = 0; i < result.length; i++) {
    	    for (j = 0; j < result[i].results[j].length; j++) {
                allPeopleArray.push(result[i].results[j]);
    
    	        reducedPeopleArray.push({
    	    	    personId:  result[i].results[j].id,
    		    firstName: result[i].results[j].first_name,
    		    lastName:  result[i].results[j].last_name
    	        });        
    	    }
        }
    
        return cb(null, {'people': reducedPeopleArray});
    }
    
    
    function errorCb(error) {
        console.log('error: ' + error);
        return cb({'error': error}, null);
    }
    
    
    
    //KICK OFF
    //make an initial call for the first page. from the response we can see how many
    //additional pages we need to call to get all the events of a nation.
    //to get additional pages we make use of downloadAllAsync function
    request(optionsForFirstRequest, callbackForFirstRequest);
}






//HELPER FUNCTION
function downloadAllAsync(urls, onsuccess, onerror) {

    var pending = urls.length;
    var result = [];

    if (pending === 0) {
	setTimeout(onsuccess.bind(null, result), 0);
	return;
    }

    urls.forEach(function (url, i) {
        downloadAsync(url, function (someThingsInAnArray) {
                if (result) {
                    result[i] = someThingsInAnArray; //store at fixed index
        	    pending--;                    //register the success
                    console.log('pending: ' + pending);
        	    if (pending === 0) {
                        onsuccess(result);
        	    } 
                }
            }, function (error) {
                console.log('downloadAsync error function. error: ' + error);
        	if (result) {
                    result = null;
        	    onerror(error);
        	}
            });
    });
}

function downloadAsync(url_, successCb, errorCb) {
    var optionsIndividual = {
	url: url_,
	method: 'GET',
	headers: {
	    //'User-Agent': userAgent,
	    'Content-Type': 'application/json',
	    'Accept': 'application/json'
	}
    };

    function callbackIndividual(error, response, body) {
        console.log('callbackIndividual');
	if (!error && response.statusCode == 200) {
	    var bodyObj = JSON.parse(body);
	    return successCb(bodyObj);
	} else {
            console.log('error in callbackIndividual: ' + error);
	    return errorCb(error);
	}
    }

    //make a call for an individual page of events 
    request(optionsIndividual, callbackIndividual);
}





//start 
readAndWork();
