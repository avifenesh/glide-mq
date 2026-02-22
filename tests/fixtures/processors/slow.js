module.exports = async (job) => {
  await new Promise((r) => setTimeout(r, job.data.delay || 500));
  return job.data;
};
