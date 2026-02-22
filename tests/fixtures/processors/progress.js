module.exports = async (job) => {
  await job.updateProgress(50);
  await job.log('halfway');
  await job.updateProgress(100);
  return job.data;
};
