const log4js = require('log4js');
let fs = require('fs');
let dir = './log';

if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir);
}

log4js.configure({
    appenders: {
        everything: { type: 'file', filename: './log/all-the-logs.log', maxLogSize: 2048, backups: 3, compress: true }
    },
    categories: {
        default: { appenders: ['everything'], level: 'all' }
    }
});

const logger = log4js.getLogger('log');

function categoryProcessor(file) {
    let lineReader = require('readline').createInterface({
        input: require('fs').createReadStream(file)
    });

    let arr = []
    let tmp = ''
    let cnt = 0
    let config = require('./config.js');
    let mysql = require('mysql');
    let connection = mysql.createConnection(config);

    connection.connect();
    let dbName = config.database;

    let command = `select * from ??.reviews_userentries limit 1
					 
             	  `
    lineReader.on('line', function(line) {
        // console.log('Line from file:', line);
        if (line[0] == '{') {
            if (tmp != '') {

                try {
                    arr.push(tmp)
                    cnt++
                    let json = JSON.parse(tmp)

                    // insert into DB
                    // if (json.coordinates && json.coordinates.latitude == '-31.4153385'){
                    console.log('==================')
                    let insertCommand = `insert into ??.categories_mapping 
	                    (categories, business_info)
						values (?, ?)		

		         			`
                    // console.log('json = ', json)
                    let category = json.categories ? json.categories[0][0][0] : ''
                    let business_info = json.business_info ? json.business_info : ''
                    let insertParametters = []

                    console.log('category = ', json.categories)
                    console.log('business_info = ', business_info)
                    logger.debug('category: ' + json.categories)
                    array = json.categories ? arrayProcessor(json.categories) : []
                    for (let i = 0; i < array.length; i++) {


                        insertParametters = [dbName, array[i], JSON.stringify(business_info)]
                        connection.query(insertCommand, insertParametters, function(err, rows, fields) {
                            if (err) throw err;
                            // console.log(rows);
                        });
                    }
                    // }

                    //
                    tmp = ''
                } catch (err) {
                    logger.error(err)
                    tmp = ''
                }
            } else {
                tmp += line
            }
        } else {
            tmp += line
        }
    });

    // connection.end();
    return arr
}

function arrayProcessor(arr) {
    ans = []

    for (let i = 0; i < arr.length; i++) {
        for (let j = 0; j < arr[i].length; j++) {
            ans.push(arr[i][j][0])
        }
    }

    return [...new Set(ans)]
}

module.exports = categoryProcessor;