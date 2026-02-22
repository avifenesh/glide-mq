// eslint-disable-next-line @typescript-eslint/no-require-imports
const { UnrecoverableError } = require('../../../dist/errors');

module.exports = async () => {
  throw new UnrecoverableError('fatal-in-sandbox');
};
