module.exports = async (job) => {
  if (job.data.crash) {
    process.exit(1);
  }
  return job.data;
};
