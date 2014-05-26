//
// worker.js: main file for working with long running processes
//

var request = require('request'),
    mongoose = require('mongoose'),
    PORT = process.env.PORT || 5001;


var mongoUri = process.env.MONGOLAB_URI ||
               'mongodb://localhost/theGreensApp';

var conn = mongoose.connect(mongoUri, function (err, res) {
    if (err) throw new Error('ERROR: ' + err);
 
    console.log('SUCCESS: connected to mongoDB: ' + mongoUri);
});


var Jobs = new mongoose.Schema({
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

    return JobsModel.find({}, function (err, jobs) {
        if (err) throw new Error(err);

        console.log('GOT JOBS TODO...');
        console.log(jobs);

        setTimeout(readAndWork, 5000);
    });
}


//start 
readAndWork();
