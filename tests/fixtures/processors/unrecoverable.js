const { UnrecoverableError } = require('../../../dist/errors');

module.exports = async () => {
  throw new UnrecoverableError('fatal-in-sandbox');
};
