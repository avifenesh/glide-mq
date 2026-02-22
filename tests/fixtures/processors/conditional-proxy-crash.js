module.exports = async (job) => {
  if (job.data.crash) {
    job.log('about to crash');
    await new Promise((resolve) => setImmediate(resolve));
    process.exit(1);
  }
  return job.data;
};
