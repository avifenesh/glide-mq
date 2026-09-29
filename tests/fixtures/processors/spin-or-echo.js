module.exports = async (job) => {
  await job.log('started');
  if (job.data.spin) {
    for (;;) {
      // Hung processor: ignores the abort signal and never yields
    }
  }
  if (job.data.delay) await new Promise((r) => setTimeout(r, job.data.delay));
  return job.data;
};
