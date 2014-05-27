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

    var query = JobsModel
                  .find({});
                  //.sort('timeAdded'); //capped collections are naturally ordered

    query.exec(function (err, jobs) {
        console.log('in exec cb');

        if (err) throw new Error(err);

        console.log('GOT JOBS TODO FROM QUERY...');
        console.log(jobs);

        //for now just use the first model in array
        var personId = parseInt(jobs[0].personId, 10),
            listId   = jobs[0].listId;

        getAllPeopleInAList(listId, personId, function (err, result) {
            console.log('in getAllPeopleInAList callback');
            if (err) throw new Error(err);

            console.log('====> results.personInList: ' + result.personInList);
            return setTimeout(readAndWork, 10000);
        });
    });

}


function getAllPeopleInAList(listId, personId, cb) {
    var perPage = 50, //seems to be the sweet spot to avoid resp timeouts
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
    	    'Content-Type': 'application/json',
    	    'Accept': 'application/json'
     	    }
        };

    
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
    
    	//see if we need to paginate to get all people 
    	if (totalPages === 1) {
    	    //DONT need to paginate

            for (var i = 0; i < results.length; i++) {
                if (personId === results[i].id) {
                    return cb(null, {'personInList': true});
                }
            }
    	    
    	    return cb(null, {'personInList': false});
    
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
    	    downloadAllAsync(extraUrls, personId, successCb, errorCb);                
    	}
    
        } else {
    	    return errorCb(response.statusCode);
        }
    }
    
    
    function successCb(result) {
        console.log('successCb called');
    
        return cb(null, {'personInList': result});
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
function downloadAllAsync(urls, personId,  onsuccess, onerror) {

    var pending = urls.length;
    var foundId = false;

    if (pending === 0) {
	setTimeout(onsuccess.bind(null, false), 0);
	return;
    }

    urls.forEach(function (url, i) {
        downloadAsync(url, function (someThingsInAnArray) {
                //console.log('someThingsInAnArray:');
                //console.log(someThingsInAnArray);
                var i, 
                    someThingsSize = someThingsInAnArray.length;

                if (foundId) return;

                for (i = 0; i < someThingsSize; i ++) {
                    console.log('typeof someThings...[i]' +typeof someThingsInAnArray[i]);
                    console.log('typeof personId: ' + typeof personId);
                    if (someThingsInAnArray[i].id === personId) {

                        foundId = true;

                        onsuccess(true);
                        return;
                    }
                }

       
                pending--;
                console.log('pending: ' + pending);

         	if (pending === 0 && !foundId) {
                    onsuccess(false);
                    return;
        	} 


            }, function (error) {
                console.log('downloadAsync error function. error: ' + error);
        	if (!foundId) {
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
