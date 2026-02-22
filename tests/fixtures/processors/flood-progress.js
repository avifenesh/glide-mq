module.exports = async (job) => {
  for (let i = 1; i <= 50; i++) {
    await job.updateProgress(i * 2);
  }
  return job.data;
};
