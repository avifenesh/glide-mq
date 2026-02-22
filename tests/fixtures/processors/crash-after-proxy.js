module.exports = async (job) => {
  job.log('about to crash');
  await new Promise((resolve) => setImmediate(resolve));
  process.exit(1);
};
