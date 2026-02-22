module.exports = async (job) => {
  job.discard();
  throw new Error('discarded-in-sandbox');
};
