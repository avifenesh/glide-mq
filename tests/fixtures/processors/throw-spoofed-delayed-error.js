module.exports = async function throwSpoofedDelayedError() {
  const err = new Error('not really delayed');
  err.name = 'DelayedError';
  throw err;
};
