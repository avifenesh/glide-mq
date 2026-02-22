module.exports = async (job) => {
  const calls = [];
  for (let i = 1; i <= 50; i++) {
    calls.push(job.updateProgress(i * 2));
  }
  await Promise.all(calls);
  return job.data;
};
