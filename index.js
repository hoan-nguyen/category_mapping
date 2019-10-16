let categoryProcessor = require('./categoryProcessor.js')

const folder = './data/'
const fs = require('fs')

fs.readdir(folder, (err, files) => {
    files.forEach(file => {
        let filePath = folder + file
        categoryProcessor(filePath)
    });
});