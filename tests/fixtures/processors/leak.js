const path = require('path');

module.exports = async () => {
  // Throw an error that contains the current working directory path
  throw new Error(`Error in ${process.cwd()}${path.sep}secret-file.txt`);
};
