module.exports = async (job) => {
  await job.log('about to crash');
  process.exit(1);
};
