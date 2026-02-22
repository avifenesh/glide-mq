module.exports = async (job) => {
  job.log('about to crash');
  process.exit(1);
};
