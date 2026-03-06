module.exports = async function throwDelayedError() {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { DelayedError } = require('../../../dist/errors');
  throw new DelayedError(654321);
};
