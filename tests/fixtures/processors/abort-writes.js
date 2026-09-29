// After the abort signal fires, tries the job writes the worker no longer
// allows and reports how each proxy call settled.
module.exports = async (job) => {
  await job.log('started');
  await new Promise((resolve) => job.abortSignal.addEventListener('abort', resolve, { once: true }));
  const outcome = {};
  for (const [name, call] of [
    ['updateProgress', () => job.updateProgress(50)],
    ['updateData', () => job.updateData({ after: 'abort' })],
    ['log', () => job.log('aborted')],
  ]) {
    try {
      await call();
      outcome[name] = 'ok';
    } catch (err) {
      outcome[name] = err.message;
    }
  }
  return outcome;
};
